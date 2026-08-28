use std::path::Path;

use gossipd_core::log::LogEntry;
use rusqlite::{params, Connection, OptionalExtension};

pub struct Contact {
    pub id: String,
    pub master_pub: [u8; 32],
    pub name: String,
    pub endpoint_id: [u8; 32],
    pub cert: Vec<u8>,
    pub addrs: Vec<String>,

    pub onion: Option<String>,
}

pub struct QueueRow {
    pub recipient: [u8; 32],
    pub seq: u64,
    pub attempts: u32,
    pub next_at: f64,
}

pub struct Store {
    conn: Connection,
}

impl Store {
    pub fn open(dir: &Path) -> rusqlite::Result<Self> {
        let conn = Connection::open(dir.join("gossip.db"))?;
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             CREATE TABLE IF NOT EXISTS meta(
               key TEXT PRIMARY KEY, value TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS contacts(
               id TEXT PRIMARY KEY,
               master_pub BLOB NOT NULL,
               name TEXT NOT NULL,
               endpoint_id BLOB NOT NULL,
               cert BLOB NOT NULL,
               addrs TEXT NOT NULL DEFAULT '[]');
             CREATE TABLE IF NOT EXISTS log(
               author BLOB NOT NULL,
               recipient BLOB NOT NULL,
               seq INTEGER NOT NULL,
               kind TEXT NOT NULL,
               body TEXT NOT NULL,
               ts REAL NOT NULL,
               sig BLOB NOT NULL,
               PRIMARY KEY(author, recipient, seq));
             CREATE TABLE IF NOT EXISTS queue(
               recipient BLOB NOT NULL,
               seq INTEGER NOT NULL,
               attempts INTEGER NOT NULL DEFAULT 0,
               next_at REAL NOT NULL DEFAULT 0,
               PRIMARY KEY(recipient, seq));",
        )?;

        let _ = conn.execute("ALTER TABLE contacts ADD COLUMN onion TEXT", []);
        Ok(Self { conn })
    }

    pub fn snapshot(&self, path: &Path) -> rusqlite::Result<()> {
        let _ = std::fs::remove_file(path);
        self.conn
            .execute("VACUUM INTO ?1", [path.to_string_lossy()])?;
        Ok(())
    }

    pub fn meta_get(&self, key: &str) -> Option<String> {
        self.conn
            .query_row("SELECT value FROM meta WHERE key=?1", [key], |r| r.get(0))
            .optional()
            .ok()
            .flatten()
    }

    pub fn meta_set(&self, key: &str, value: &str) {
        self.conn
            .execute(
                "INSERT INTO meta(key,value) VALUES(?1,?2)
                 ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                params![key, value],
            )
            .expect("meta write");
    }

    pub fn upsert_contact(&self, c: &Contact) {
        self.conn
            .execute(
                "INSERT INTO contacts(id,master_pub,name,endpoint_id,cert,addrs,onion)
                 VALUES(?1,?2,?3,?4,?5,?6,?7)
                 ON CONFLICT(id) DO UPDATE SET
                   name=excluded.name, endpoint_id=excluded.endpoint_id,
                   cert=excluded.cert, addrs=excluded.addrs, onion=excluded.onion",
                params![
                    c.id,
                    c.master_pub,
                    c.name,
                    c.endpoint_id,
                    c.cert,
                    serde_json::to_string(&c.addrs).unwrap(),
                    c.onion,
                ],
            )
            .expect("contact write");
    }

    pub fn contacts(&self) -> Vec<Contact> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT id,master_pub,name,endpoint_id,cert,addrs,onion FROM contacts ORDER BY name",
            )
            .expect("contacts query");
        let rows = stmt
            .query_map([], |r| {
                Ok(Contact {
                    id: r.get(0)?,
                    master_pub: r.get::<_, Vec<u8>>(1)?.try_into().unwrap_or([0; 32]),
                    name: r.get(2)?,
                    endpoint_id: r.get::<_, Vec<u8>>(3)?.try_into().unwrap_or([0; 32]),
                    cert: r.get(4)?,
                    addrs: serde_json::from_str(&r.get::<_, String>(5)?).unwrap_or_default(),
                    onion: r.get::<_, Option<String>>(6)?.filter(|s| !s.is_empty()),
                })
            })
            .expect("contacts rows");
        rows.filter_map(Result::ok).collect()
    }

    pub fn contact(&self, id: &str) -> Option<Contact> {
        self.contacts().into_iter().find(|c| c.id == id)
    }

    pub fn contact_by_master(&self, master_pub: &[u8; 32]) -> Option<Contact> {
        self.contacts()
            .into_iter()
            .find(|c| &c.master_pub == master_pub)
    }

    pub fn append(&self, e: &LogEntry) -> bool {
        self.conn
            .execute(
                "INSERT OR IGNORE INTO log(author,recipient,seq,kind,body,ts,sig)
                 VALUES(?1,?2,?3,?4,?5,?6,?7)",
                params![
                    e.author,
                    e.recipient,
                    e.seq as i64,
                    e.kind,
                    e.body,
                    e.ts,
                    e.sig
                ],
            )
            .expect("log append")
            > 0
    }

    pub fn frontier(&self, author: &[u8; 32], recipient: &[u8; 32]) -> u64 {
        self.conn
            .query_row(
                "SELECT COALESCE(MAX(seq),0) FROM log WHERE author=?1 AND recipient=?2",
                params![author, recipient],
                |r| r.get::<_, i64>(0),
            )
            .unwrap_or(0) as u64
    }

    pub fn entries_after(
        &self,
        author: &[u8; 32],
        recipient: &[u8; 32],
        seq: u64,
    ) -> Vec<LogEntry> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT author,recipient,seq,kind,body,ts,sig FROM log
                 WHERE author=?1 AND recipient=?2 AND seq>?3 ORDER BY seq",
            )
            .expect("entries query");
        let rows = stmt
            .query_map(params![author, recipient, seq as i64], row_to_entry)
            .expect("entries rows");
        rows.filter_map(Result::ok).collect()
    }

    pub fn history(&self, me: &[u8; 32], peer: &[u8; 32], limit: u32) -> Vec<LogEntry> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT author,recipient,seq,kind,body,ts,sig FROM log
                 WHERE (author=?1 AND recipient=?2) OR (author=?2 AND recipient=?1)
                 ORDER BY ts DESC LIMIT ?3",
            )
            .expect("history query");
        let mut entries: Vec<LogEntry> = stmt
            .query_map(params![me, peer, limit], row_to_entry)
            .expect("history rows")
            .filter_map(Result::ok)
            .collect();
        entries.reverse();
        entries
    }

    pub fn enqueue(&self, recipient: &[u8; 32], seq: u64) {
        self.conn
            .execute(
                "INSERT OR IGNORE INTO queue(recipient,seq) VALUES(?1,?2)",
                params![recipient, seq as i64],
            )
            .expect("enqueue");
    }

    pub fn dequeue_up_to(&self, recipient: &[u8; 32], seq: u64) -> Vec<u64> {
        let flushed: Vec<u64> = {
            let mut stmt = self
                .conn
                .prepare("SELECT seq FROM queue WHERE recipient=?1 AND seq<=?2 ORDER BY seq")
                .expect("queue query");
            stmt.query_map(params![recipient, seq as i64], |r| {
                r.get::<_, i64>(0).map(|v| v as u64)
            })
            .expect("queue rows")
            .filter_map(Result::ok)
            .collect()
        };
        self.conn
            .execute(
                "DELETE FROM queue WHERE recipient=?1 AND seq<=?2",
                params![recipient, seq as i64],
            )
            .expect("dequeue");
        flushed
    }

    pub fn queue_set_attempt(&self, recipient: &[u8; 32], attempts: u32, next_at: f64) {
        self.conn
            .execute(
                "UPDATE queue SET attempts=?2, next_at=?3 WHERE recipient=?1",
                params![recipient, attempts, next_at],
            )
            .expect("queue update");
    }

    pub fn queued(&self) -> Vec<QueueRow> {
        let mut stmt = self
            .conn
            .prepare("SELECT recipient,seq,attempts,next_at FROM queue ORDER BY recipient,seq")
            .expect("queued query");
        let rows = stmt
            .query_map([], |r| {
                Ok(QueueRow {
                    recipient: r.get::<_, Vec<u8>>(0)?.try_into().unwrap_or([0; 32]),
                    seq: r.get::<_, i64>(1)? as u64,
                    attempts: r.get(2)?,
                    next_at: r.get(3)?,
                })
            })
            .expect("queued rows");
        rows.filter_map(Result::ok).collect()
    }

    pub fn queued_for(&self, recipient: &[u8; 32]) -> Vec<QueueRow> {
        self.queued()
            .into_iter()
            .filter(|q| &q.recipient == recipient)
            .collect()
    }
}

fn row_to_entry(r: &rusqlite::Row<'_>) -> rusqlite::Result<LogEntry> {
    Ok(LogEntry {
        author: r.get::<_, Vec<u8>>(0)?.try_into().unwrap_or([0; 32]),
        recipient: r.get::<_, Vec<u8>>(1)?.try_into().unwrap_or([0; 32]),
        seq: r.get::<_, i64>(2)? as u64,
        kind: r.get(3)?,
        body: r.get(4)?,
        ts: r.get(5)?,
        sig: r.get(6)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use gossipd_core::identity::MasterKey;

    fn store() -> Store {
        let dir = tempdir();
        Store::open(&dir).unwrap()
    }

    fn tempdir() -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "gossipd-test-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let _ = std::fs::remove_file(dir.join("gossip.db"));
        dir
    }

    #[test]
    fn log_append_frontier_and_queue_flush() {
        let s = store();
        let me = MasterKey::from_bytes(&[1; 32]);
        let peer = [9; 32];
        for seq in 1..=3 {
            let e = LogEntry::sign(&me, peer, seq, "chat", &format!("m{seq}"), seq as f64);
            assert!(s.append(&e));
            assert!(!s.append(&e), "append must be idempotent");
            s.enqueue(&peer, seq);
        }
        assert_eq!(s.frontier(&me.public().to_bytes(), &peer), 3);
        assert_eq!(s.entries_after(&me.public().to_bytes(), &peer, 1).len(), 2);
        assert_eq!(s.queued().len(), 3);
        assert_eq!(s.dequeue_up_to(&peer, 2), vec![1, 2]);
        assert_eq!(s.queued().len(), 1);
    }
}

#[cfg(test)]
mod sig_roundtrip {
    use super::*;
    use gossipd_core::identity::MasterKey;
    use gossipd_core::log::LogEntry;

    #[test]
    fn store_roundtrip_preserves_sig() {
        let dir = std::env::temp_dir().join(format!("gossipd-sig-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let _ = std::fs::remove_file(dir.join("gossip.db"));
        let s = Store::open(&dir).unwrap();
        let k = MasterKey::from_bytes(&[1; 32]);
        let e = LogEntry::sign(&k, [2; 32], 1, "chat", "hello bob", 1755741180.2868862);
        assert!(s.append(&e));
        let back = s.entries_after(&e.author, &e.recipient, 0).remove(0);
        assert_eq!(back, e);
        assert!(back.verify(), "sqlite roundtrip broke the signature");
    }
}
