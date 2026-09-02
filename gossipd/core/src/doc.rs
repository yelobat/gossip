use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use yrs::branch::BranchPtr;
use yrs::types::Delta;
use yrs::updates::decoder::Decode;
use yrs::updates::encoder::Encode;
use yrs::{
    Any, Assoc, Doc, GetString, Map, MapRef, Observable, OffsetKind, Options, Out, ReadTxn,
    StateVector, StickyIndex, Text, TextRef, Transact, Update,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Op(pub usize, pub usize, pub String);

pub const CORRUPT: &str = "corrupting update";

const MAX_CLOCK: u32 = 1 << 31;

pub const MAX_CHARS: usize = 4 * 1024 * 1024;

const TEXT: &str = "text";
const MEMBERS: &str = "members";
const ORIGIN_CLIENT: &str = "client";
const ORIGIN_REMOTE: &str = "remote";

pub struct SharedDoc {
    main: Doc,
    text: TextRef,
    members: MapRef,
    shadows: HashMap<u64, Shadow>,
    cursors: HashMap<String, PeerCursor>,
}

struct Shadow {
    doc: Doc,
    text: TextRef,
    mirror: String,
    dirty_notified: bool,
}

struct PeerCursor {
    name: String,
    index: StickyIndex,
}

#[derive(Debug)]
pub struct SyncOut {
    pub ops: Vec<Op>,
    pub size: usize,
    pub update: Option<Vec<u8>>,
    pub cursor: Option<Vec<u8>>,
    pub peers: Vec<(String, String, usize)>,
}

impl Default for SharedDoc {
    fn default() -> Self {
        Self::new()
    }
}

fn new_doc() -> Doc {
    Doc::with_options(Options {
        offset_kind: OffsetKind::Utf16,
        ..Default::default()
    })
}

impl SharedDoc {
    pub fn new() -> Self {
        let main = new_doc();
        let text = main.get_or_insert_text(TEXT);
        let members = main.get_or_insert_map(MEMBERS);
        Self {
            main,
            text,
            members,
            shadows: HashMap::new(),
            cursors: HashMap::new(),
        }
    }

    pub fn load<'a>(
        snapshot: Option<&[u8]>,
        updates: impl IntoIterator<Item = &'a [u8]>,
    ) -> Result<Self, String> {
        let doc = Self::new();
        if let Some(s) = snapshot {
            doc.apply_remote(s)?;
        }
        for u in updates {
            doc.apply_remote(u)?;
        }
        Ok(doc)
    }

    pub fn full_state(&self) -> Vec<u8> {
        self.main
            .transact()
            .encode_state_as_update_v1(&StateVector::default())
    }

    pub fn state_vector(&self) -> Vec<u8> {
        self.main.transact().state_vector().encode_v1()
    }

    pub fn diff(&self, sv: &[u8]) -> Result<Vec<u8>, String> {
        let sv = guarded("state vector", || {
            plausible(sv, 1)?;
            StateVector::decode_v1(sv).map_err(|e| format!("bad state vector: {e}"))
        })?;
        guarded("diff", || Ok(self.main.transact().encode_state_as_update_v1(&sv)))
    }

    pub fn apply_remote(&self, update: &[u8]) -> Result<bool, String> {
        let update = guarded("update", || {
            plausible(update, 2)?;
            Update::decode_v1(update).map_err(|e| format!("bad update: {e}"))
        })?;
        let lower = update.state_vector_lower();
        let touched: Vec<yrs::ClientID> = lower.iter().map(|(c, _)| *c).collect();
        {
            let local = self.main.transact().state_vector();
            for (c, low) in lower.iter() {
                if *low > local.get(c) {
                    return Err("bad update: starts past our state".into());
                }
            }
        }

        let inserted = update.insertions(true);
        for c in &touched {
            let ranges: Vec<std::ops::Range<u32>> = inserted
                .get(c)
                .map(|r| r.iter().map(|(x, _)| x.clone()).collect())
                .unwrap_or_default();
            let total: u32 = ranges.iter().map(|x| x.end.saturating_sub(x.start)).sum();
            if total == 0 {
                return Err("bad update: empty block".into());
            }
            if ranges.len() > 1 {
                return Err("bad update: blocks not contiguous".into());
            }
            if ranges.iter().any(|x| x.end > MAX_CLOCK) {
                return Err("bad update: absurd clock".into());
            }
        }
        let changed = guarded("update", || {
            let mut txn = self.main.transact_mut_with(ORIGIN_REMOTE);
            txn.apply_update(update)
                .map_err(|e| format!("{CORRUPT}: update rejected: {e}"))?;
            txn.commit();
            if txn.has_missing_updates() {
                return Err(format!("{CORRUPT}: update left blocks pending"));
            }
            Ok(txn.before_state() != txn.after_state() || !txn.delete_set().is_empty())
        })?;
        if !changed {
            return Ok(false);
        }

        let probe = {
            let txn = self.main.transact();
            let mut sv = StateVector::default();
            for (c, clock) in txn.state_vector().iter() {
                if !touched.contains(c) {
                    sv.inc_by(*c, *clock);
                }
            }
            guarded("update", || Ok(txn.encode_state_as_update_v1(&sv)))
        };
        match probe {
            Ok(_) => Ok(changed),
            Err(e) => Err(format!("{CORRUPT}: {e}")),
        }
    }

    pub fn text(&self) -> String {
        self.text.get_string(&self.main.transact())
    }

    pub fn set_member(&self, id: &str, name: &str) -> Vec<u8> {
        let mut txn = self.main.transact_mut_with(ORIGIN_CLIENT);
        self.members.insert(&mut txn, id, Any::from(name));
        txn.commit();
        txn.encode_update_v1()
    }

    pub fn members(&self) -> Vec<(String, String)> {
        let txn = self.main.transact();
        let mut out: Vec<(String, String)> = self
            .members
            .iter(&txn)
            .map(|(k, v)| (k.to_string(), v.to_string(&txn)))
            .collect();
        out.sort();
        out
    }

    pub fn is_member(&self, id: &str) -> bool {
        self.members.get(&self.main.transact(), id).is_some()
    }

    pub fn attach(&mut self, client: u64) -> String {
        let doc = new_doc();
        let text = doc.get_or_insert_text(TEXT);
        doc.get_or_insert_map(MEMBERS);
        {
            let mut txn = doc.transact_mut_with(ORIGIN_REMOTE);
            let full = Update::decode_v1(&self.full_state()).expect("own state decodes");
            txn.apply_update(full).expect("own state applies");
        }
        let mirror = text.get_string(&doc.transact());
        self.shadows.insert(
            client,
            Shadow {
                doc,
                text,
                mirror: mirror.clone(),
                dirty_notified: false,
            },
        );
        mirror
    }

    pub fn detach(&mut self, client: u64) {
        self.shadows.remove(&client);
    }

    pub fn attached(&self) -> Vec<u64> {
        self.shadows.keys().copied().collect()
    }

    pub fn is_attached(&self, client: u64) -> bool {
        self.shadows.contains_key(&client)
    }

    pub fn mark_dirty(&mut self) -> Vec<u64> {
        self.mark_dirty_except(u64::MAX)
    }

    pub fn mark_dirty_except(&mut self, client: u64) -> Vec<u64> {
        let mut out = vec![];
        for (id, s) in self.shadows.iter_mut() {
            if *id != client && !s.dirty_notified {
                s.dirty_notified = true;
                out.push(*id);
            }
        }
        out
    }

    pub fn set_peer_cursor(&mut self, peer: &str, name: &str, index: &[u8]) -> Result<(), String> {
        let index = guarded("cursor", || {
            plausible(index, 1)?;
            StickyIndex::decode_v1(index).map_err(|e| format!("bad cursor: {e}"))
        })?;
        self.cursors.insert(
            peer.to_string(),
            PeerCursor {
                name: name.to_string(),
                index,
            },
        );
        Ok(())
    }

    pub fn clear_peer_cursor(&mut self, peer: &str) {
        self.cursors.remove(peer);
    }

    pub fn sync(
        &mut self,
        client: u64,
        ops: &[Op],
        cursor: Option<usize>,
    ) -> Result<SyncOut, String> {
        let result = self.sync_inner(client, ops, cursor);
        if result.is_err() {
            self.shadows.remove(&client);
        }
        result
    }

    fn sync_inner(
        &mut self,
        client: u64,
        ops: &[Op],
        cursor: Option<usize>,
    ) -> Result<SyncOut, String> {
        let shadow = self
            .shadows
            .get_mut(&client)
            .ok_or_else(|| "not attached".to_string())?;
        shadow.dirty_notified = false;

        let update = if ops.is_empty() {
            None
        } else {
            let mut txn = shadow.doc.transact_mut_with(ORIGIN_CLIENT);
            for op in ops {
                let Op(pos, del, ins) = op;
                let (start_b, start_u) = locate(&shadow.mirror, *pos)
                    .ok_or_else(|| format!("op at {pos} past end"))?;
                let (end_b, end_u) = locate(&shadow.mirror[start_b..], *del)
                    .map(|(b, u)| (start_b + b, start_u + u))
                    .ok_or_else(|| format!("op deletes {del} past end at {pos}"))?;
                if *del > 0 {
                    shadow
                        .text
                        .remove_range(&mut txn, start_u as u32, (end_u - start_u) as u32);
                    shadow.mirror.replace_range(start_b..end_b, "");
                }
                if !ins.is_empty() {
                    shadow.text.insert(&mut txn, start_u as u32, ins);
                    shadow.mirror.insert_str(start_b, ins);
                }
                if shadow.mirror.len() > MAX_CHARS * 4 {
                    return Err("document too large".into());
                }
            }
            txn.commit();
            Some(txn.encode_update_v1())
        };

        if let Some(u) = &update {
            let mut txn = self.main.transact_mut_with(ORIGIN_CLIENT);
            txn.apply_update(Update::decode_v1(u).map_err(|e| e.to_string())?)
                .map_err(|e| e.to_string())?;
        }

        let diff = {
            let sv = shadow.doc.transact().state_vector();
            self.main.transact().encode_state_as_update_v1(&sv)
        };
        let captured: Arc<Mutex<Vec<Delta>>> = Arc::default();
        {
            let sink = captured.clone();
            let sub = shadow.text.observe(move |txn, ev| {
                sink.lock().unwrap().extend(ev.delta(txn).iter().cloned());
            });
            let mut txn = shadow.doc.transact_mut_with(ORIGIN_REMOTE);
            txn.apply_update(Update::decode_v1(&diff).map_err(|e| e.to_string())?)
                .map_err(|e| e.to_string())?;
            txn.commit();
            drop(txn);
            drop(sub);
        }
        let deltas = std::mem::take(&mut *captured.lock().unwrap());
        let ops_out = delta_to_ops(&mut shadow.mirror, &deltas)?;
        debug_assert_eq!(shadow.mirror, shadow.text.get_string(&shadow.doc.transact()));

        let cursor_bytes = cursor.and_then(|c| {
            let (_, u) = locate(&shadow.mirror, c)?;
            let txn = shadow.doc.transact();
            let branch = BranchPtr::from(AsRef::<yrs::branch::Branch>::as_ref(&shadow.text));
            let idx = StickyIndex::at(&txn, branch, u as u32, Assoc::After)
                .or_else(|| StickyIndex::at(&txn, branch, u as u32, Assoc::Before))?;
            Some(idx.encode_v1())
        });
        let peers = {
            let txn = shadow.doc.transact();
            self.cursors
                .iter()
                .filter_map(|(id, c)| {
                    let off = guarded("cursor", || Ok(c.index.get_offset(&txn))).ok()??;
                    let (_, ch) = locate_u16(&shadow.mirror, off.index as usize)?;
                    Some((id.clone(), c.name.clone(), ch))
                })
                .collect()
        };

        Ok(SyncOut {
            ops: ops_out,
            size: shadow.mirror.chars().count(),
            update,
            cursor: cursor_bytes,
            peers,
        })
    }
}

fn plausible(bytes: &[u8], counts: usize) -> Result<(), String> {
    let mut at = 0;
    let mut varint = || {
        let mut v: u64 = 0;
        for i in 0..10 {
            let b = *bytes.get(at)?;
            at += 1;
            v |= u64::from(b & 0x7f) << (7 * i);
            if b & 0x80 == 0 {
                return Some(v);
            }
        }
        None
    };
    for _ in 0..counts {
        match varint() {
            Some(v) if v as usize <= bytes.len() => {}
            _ => return Err("implausible length".into()),
        }
    }
    Ok(())
}

fn guarded<T>(what: &str, f: impl FnOnce() -> Result<T, String>) -> Result<T, String> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(f))
        .unwrap_or_else(|_| Err(format!("malformed {what}")))
}

fn locate(s: &str, ch: usize) -> Option<(usize, usize)> {
    let mut u = 0;
    for (i, (b, c)) in s.char_indices().enumerate() {
        if i == ch {
            return Some((b, u));
        }
        u += c.len_utf16();
    }
    (s.chars().count() == ch).then_some((s.len(), u))
}

fn locate_u16(s: &str, unit: usize) -> Option<(usize, usize)> {
    let mut u = 0;
    for (i, (b, c)) in s.char_indices().enumerate() {
        if u == unit {
            return Some((b, i));
        }
        if u > unit {
            return None;
        }
        u += c.len_utf16();
    }
    (u == unit).then_some((s.len(), s.chars().count()))
}

fn delta_to_ops(mirror: &mut String, deltas: &[Delta]) -> Result<Vec<Op>, String> {
    let mut ops = vec![];
    let mut u = 0usize;
    for d in deltas {
        match d {
            Delta::Retain(n, _) => u += *n as usize,
            Delta::Inserted(v, _) => {
                let s = match v {
                    Out::Any(Any::String(s)) => s.to_string(),
                    other => return Err(format!("non-text content in document: {other:?}")),
                };
                let (b, ch) = locate_u16(mirror, u).ok_or("delta inserts outside the text")?;
                ops.push(Op(ch, 0, s.clone()));
                mirror.insert_str(b, &s);
                u += s.encode_utf16().count();
            }
            Delta::Deleted(n) => {
                let (b, ch) = locate_u16(mirror, u).ok_or("delta deletes outside the text")?;
                let (len_b, del) =
                    locate_u16(&mirror[b..], *n as usize).ok_or("delta deletes past the end")?;
                ops.push(Op(ch, del, String::new()));
                mirror.replace_range(b..b + len_b, "");
            }
        }
    }
    Ok(ops)
}

pub fn apply_ops(buf: &mut String, ops: &[Op]) -> Result<(), String> {
    for Op(pos, del, ins) in ops {
        let (start, _) = locate(buf, *pos).ok_or("pos past end")?;
        let (len, _) = locate(&buf[start..], *del).ok_or("del past end")?;
        buf.replace_range(start..start + len, ins);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn s(x: &str) -> String {
        x.to_string()
    }

    #[test]
    fn local_edits_roundtrip_with_multibyte() {
        let mut d = SharedDoc::new();
        assert_eq!(d.attach(1), "");
        let out = d.sync(1, &[Op(0, 0, s("héllo ★ wörld"))], Some(3)).unwrap();
        assert!(out.ops.is_empty(), "own edits never echo back");
        assert_eq!(out.size, 13);
        assert!(out.update.is_some());
        assert!(out.cursor.is_some());
        assert_eq!(d.text(), "héllo ★ wörld");
        let out = d.sync(1, &[Op(6, 1, s("☆☆")), Op(0, 1, s(""))], None).unwrap();
        assert!(out.ops.is_empty());
        assert_eq!(d.text(), "éllo ☆☆ wörld");
    }

    #[test]
    fn remote_change_comes_back_as_char_ops() {
        let mut a = SharedDoc::new();
        let mut b = SharedDoc::new();
        a.attach(1);
        b.attach(2);
        let mut buf_b = String::new();
        let up = a.sync(1, &[Op(0, 0, s("ab★cd"))], None).unwrap().update.unwrap();
        assert!(b.apply_remote(&up).unwrap());
        let out = b.sync(2, &[], None).unwrap();
        apply_ops(&mut buf_b, &out.ops).unwrap();
        assert_eq!(buf_b, "ab★cd");
        let up_b = b.sync(2, &[Op(2, 1, s(""))], None).unwrap().update.unwrap();
        apply_ops(&mut buf_b, &[Op(2, 1, s(""))]).unwrap();
        let up_a = a.sync(1, &[Op(3, 0, s("X"))], None).unwrap().update.unwrap();
        a.apply_remote(&up_b).unwrap();
        b.apply_remote(&up_a).unwrap();
        let out = b.sync(2, &[], None).unwrap();
        apply_ops(&mut buf_b, &out.ops).unwrap();
        assert_eq!(buf_b, "abXcd");
        assert_eq!(a.text(), "abXcd");
        assert_eq!(b.text(), "abXcd");
    }

    #[test]
    fn four_byte_allocation_bomb_is_rejected() {
        let bomb = [221u8, 221, 203, 127];
        let mut d = SharedDoc::new();
        assert_eq!(d.apply_remote(&bomb).unwrap_err(), "implausible length");
        assert_eq!(d.diff(&bomb).unwrap_err(), "implausible length");
        assert!(d.set_peer_cursor("p", "n", &bomb).is_err());
        let bomb = [1u8, 129, 255, 255, 65, 41, 1, 1, 0];
        assert_eq!(d.apply_remote(&bomb).unwrap_err(), "implausible length");
        let poison = [1u8, 1, 1, 1, 0, 0, 0];
        assert!(d.apply_remote(&poison).unwrap_err().starts_with("bad update"));
        assert!(d.diff(&[0]).is_ok());
        let parked = [
            1u8, 1, 223, 49, 3, 66, 43, 1, 1, 1, 3, 3, 3, 3, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 143,
        ];
        let err = d.apply_remote(&parked).unwrap_err();
        assert!(err.starts_with("bad update"), "{err}");
        assert!(d.diff(&[0]).is_ok());
        let gap = [2u8, 1, 8, 167, 36, 0, 4, 0, 8, 175, 4, 0, 1, 25, 25, 249, 17, 65, 255, 255];
        assert_eq!(d.apply_remote(&gap).unwrap_err(), "bad update: starts past our state");
        let wrapped = [
            1u8, 1, 1, 255, 255, 255, 255, 11, 0, 1, 1, 1, 7, 41, 41, 65, 129, 255, 15, 1, 35, 125,
            41, 169, 65, 3, 3, 3, 91, 193, 65, 129, 1, 171, 255,
        ];
        let err = d.apply_remote(&wrapped).unwrap_err();
        assert!(err.starts_with("bad update"), "{err}");
        assert!(d.diff(&[0]).is_ok());
        let bomb = [165u8, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0];
        assert_eq!(d.apply_remote(&bomb).unwrap_err(), "implausible length");
        assert_eq!(d.diff(&bomb).unwrap_err(), "implausible length");
        assert!(d.apply_remote(&[0, 0]).is_ok());
        assert!(d.diff(&[0]).is_ok());
    }

    #[test]
    fn bad_ops_drop_the_shadow() {
        let mut d = SharedDoc::new();
        d.attach(7);
        assert!(d.sync(7, &[Op(5, 0, s("x"))], None).is_err());
        assert!(!d.is_attached(7));
        assert_eq!(d.sync(7, &[], None).unwrap_err(), "not attached");
        d.attach(7);
        assert!(d.sync(7, &[Op(0, 1, s(""))], None).is_err());
    }

    #[test]
    fn dirty_is_coalesced_until_next_sync() {
        let mut d = SharedDoc::new();
        d.attach(1);
        d.attach(2);
        let mut got = d.mark_dirty();
        got.sort();
        assert_eq!(got, vec![1, 2]);
        assert!(d.mark_dirty().is_empty());
        d.sync(1, &[], None).unwrap();
        assert!(d.mark_dirty_except(1).is_empty(), "the syncing client keeps its clean flag");
        assert_eq!(d.mark_dirty(), vec![1]);
    }

    #[test]
    fn members_and_cursors_sync_across_docs() {
        let mut a = SharedDoc::new();
        let mut b = SharedDoc::new();
        let up = a.set_member("gsp1-a", "alice");
        b.apply_remote(&up).unwrap();
        assert!(b.is_member("gsp1-a"));
        assert!(!b.is_member("gsp1-b"));
        assert_eq!(b.members(), vec![(s("gsp1-a"), s("alice"))]);

        a.attach(1);
        b.attach(2);
        let out = a.sync(1, &[Op(0, 0, s("hello"))], Some(5)).unwrap();
        b.apply_remote(&out.update.unwrap()).unwrap();
        b.set_peer_cursor("gsp1-a", "alice", &out.cursor.unwrap()).unwrap();
        let out = b.sync(2, &[], None).unwrap();
        assert_eq!(out.peers, vec![(s("gsp1-a"), s("alice"), 5)]);
        let out = b.sync(2, &[Op(0, 0, s("★★"))], None).unwrap();
        assert_eq!(out.peers[0].2, 7);
        let out = a.sync(1, &[], Some(0)).unwrap();
        assert!(out.cursor.is_some());
        assert!(b.set_peer_cursor("x", "y", b"garbage").is_err());
    }

    #[test]
    fn cursors_survive_multibyte_and_astral_text() {
        let mut a = SharedDoc::new();
        let mut b = SharedDoc::new();
        a.attach(1);
        b.attach(2);
        let out = a.sync(1, &[Op(0, 0, s("x檴😀y"))], Some(3)).unwrap();
        b.apply_remote(&out.update.unwrap()).unwrap();
        b.set_peer_cursor("a", "alice", &out.cursor.unwrap()).unwrap();
        let mut buf_b = String::new();
        let out = b.sync(2, &[], None).unwrap();
        apply_ops(&mut buf_b, &out.ops).unwrap();
        assert_eq!(buf_b, "x檴😀y");
        assert_eq!(out.peers[0].2, 3, "cursor after the emoji is char 3");
        for c in 0..=4 {
            let out = a.sync(1, &[], Some(c)).unwrap();
            b.set_peer_cursor("a", "alice", &out.cursor.unwrap()).unwrap();
            assert_eq!(b.sync(2, &[], None).unwrap().peers[0].2, c);
        }
        let out = b.sync(2, &[Op(1, 2, s("-"))], None).unwrap();
        a.apply_remote(&out.update.unwrap()).unwrap();
        let mut buf_a = s("x檴😀y");
        apply_ops(&mut buf_a, &a.sync(1, &[], None).unwrap().ops).unwrap();
        assert_eq!(buf_a, "x-y");
    }

    #[test]
    fn diff_and_load_roundtrip() {
        let mut a = SharedDoc::new();
        a.attach(1);
        let u1 = a.sync(1, &[Op(0, 0, s("one"))], None).unwrap().update.unwrap();
        let u2 = a.sync(1, &[Op(3, 0, s(" two"))], None).unwrap().update.unwrap();
        let b = SharedDoc::load(Some(&a.full_state()), []).unwrap();
        assert_eq!(b.text(), "one two");
        let c = SharedDoc::load(None, [u1.as_slice(), u2.as_slice()]).unwrap();
        assert_eq!(c.text(), "one two");
        let d = SharedDoc::new();
        let missing = a.diff(&d.state_vector()).unwrap();
        d.apply_remote(&missing).unwrap();
        assert_eq!(d.text(), "one two");
        assert!(!d.apply_remote(&a.diff(&d.state_vector()).unwrap()).unwrap());
        assert!(SharedDoc::load(Some(b"\xff\xff"), []).is_err());
        assert!(a.diff(b"\xff").is_err());
    }

    #[derive(Debug, Clone)]
    enum Step {
        Edit { who: usize, pos: usize, del: usize, ins: String },
        Sync(usize),
        Exchange,
    }

    fn step() -> impl Strategy<Value = Step> {
        prop_oneof![
            5 => (0..2usize, 0..40usize, 0..4usize, "[a-c★\n]{0,3}")
                .prop_map(|(who, pos, del, ins)| Step::Edit { who, pos, del, ins }),
            3 => (0..2usize).prop_map(Step::Sync),
            2 => Just(Step::Exchange),
        ]
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(300))]
        #[test]
        fn clients_track_shadows_and_replicas_converge(steps in prop::collection::vec(step(), 1..60)) {
            let mut docs = [SharedDoc::new(), SharedDoc::new()];
            let mut bufs = [String::new(), String::new()];
            for (i, d) in docs.iter_mut().enumerate() { d.attach(i as u64); }
            let mut pending: [Vec<Op>; 2] = [vec![], vec![]];

            for st in &steps {
                match st {
                    Step::Edit { who, pos, del, ins } => {
                        let n = bufs[*who].chars().count();
                        let pos = pos.min(&n);
                        let del = (*del).min(n - pos);
                        let op = Op(*pos, del, ins.clone());
                        apply_ops(&mut bufs[*who], std::slice::from_ref(&op)).unwrap();
                        pending[*who].push(op);
                    }
                    Step::Sync(who) => {
                        let ops = std::mem::take(&mut pending[*who]);
                        let out = docs[*who].sync(*who as u64, &ops, Some(0)).unwrap();
                        apply_ops(&mut bufs[*who], &out.ops).unwrap();
                        prop_assert_eq!(bufs[*who].chars().count(), out.size);
                        prop_assert_eq!(&bufs[*who], &docs[*who].shadows[&(*who as u64)].mirror);
                    }
                    Step::Exchange => {
                        let d01 = docs[0].diff(&docs[1].state_vector()).unwrap();
                        let d10 = docs[1].diff(&docs[0].state_vector()).unwrap();
                        docs[1].apply_remote(&d01).unwrap();
                        docs[0].apply_remote(&d10).unwrap();
                    }
                }
            }
            for round in 0..2 {
                for who in 0..2 {
                    let ops = std::mem::take(&mut pending[who]);
                    let out = docs[who].sync(who as u64, &ops, None).unwrap();
                    apply_ops(&mut bufs[who], &out.ops).unwrap();
                }
                if round == 0 {
                    let d01 = docs[0].diff(&docs[1].state_vector()).unwrap();
                    let d10 = docs[1].diff(&docs[0].state_vector()).unwrap();
                    docs[1].apply_remote(&d01).unwrap();
                    docs[0].apply_remote(&d10).unwrap();
                }
            }
            prop_assert_eq!(docs[0].text(), docs[1].text());
            prop_assert_eq!(&bufs[0], &docs[0].text());
            prop_assert_eq!(&bufs[1], &docs[1].text());
        }

        #[test]
        fn garbage_never_panics(bytes in prop::collection::vec(any::<u8>(), 0..64)) {
            let mut d = SharedDoc::new();
            let _ = d.apply_remote(&bytes);
            let _ = d.diff(&bytes);
            let _ = d.set_peer_cursor("p", "n", &bytes);
            let _ = SharedDoc::load(Some(&bytes), [bytes.as_slice()]);
            d.attach(1);
            let _ = d.sync(1, &[Op(bytes.len(), bytes.len() % 3, String::from_utf8_lossy(&bytes).into())], Some(1));
        }
    }
}

