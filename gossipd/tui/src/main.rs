use std::io::{BufRead, Write};

use chrono::{Local, TimeZone};
use gossip_client::serde_json::{json, Value};
use gossip_client::{Client, Notification};

enum Command {
    Help,
    Id,
    Ticket,
    Add(String, Option<String>),
    Contacts,
    To(String),
    History(u32),
    File(String),
    Quit,
    Say(String),
    Downloads,
    SaveDir(String),
    Export(String),
    Files(String),
    Allow(String),
    Block(String),
    Ask(String),
    Accept(String),
    Decline(String),
    Empty,
    Unknown(String),
}

fn parse_command(line: &str) -> Command {
    let line = line.trim();
    if line.is_empty() {
        return Command::Empty;
    }
    if !line.starts_with('/') {
        return Command::Say(line.to_string());
    }
    let mut parts = line.splitn(2, char::is_whitespace);
    let cmd = parts.next().unwrap_or("");
    let rest = parts.next().unwrap_or("").trim();
    match cmd {
        "/help" | "/h" | "/?" => Command::Help,
        "/id" => Command::Id,
        "/ticket" => Command::Ticket,
        "/contacts" | "/c" => Command::Contacts,
        "/quit" | "/q" | "/exit" => Command::Quit,
        "/to" => Command::To(rest.to_string()),
        "/file" => Command::File(rest.to_string()),
        "/downloads" => Command::Downloads,
        "/savedir" => Command::SaveDir(rest.to_string()),
        "/export" => Command::Export(rest.to_string()),
        "/files" => Command::Files(rest.to_string()),
        "/allow" => Command::Allow(rest.to_string()),
        "/block" => Command::Block(rest.to_string()),
        "/ask" => Command::Ask(rest.to_string()),
        "/accept" => Command::Accept(rest.to_string()),
        "/decline" => Command::Decline(rest.to_string()),
        "/history" => Command::History(rest.parse().unwrap_or(20)),
        "/add" => {
            let mut a = rest.splitn(2, char::is_whitespace);
            let ticket = a.next().unwrap_or("").to_string();
            let name = a.next().map(|n| n.trim().to_string()).filter(|n| !n.is_empty());
            Command::Add(ticket, name)
        }
        other => Command::Unknown(other.to_string()),
    }
}

fn hhmm(ts: f64) -> String {
    match Local.timestamp_opt(ts as i64, 0).single() {
        Some(dt) => dt.format("%H:%M").to_string(),
        None => "--:--".to_string(),
    }
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn default_data_dir() -> String {
    if cfg!(windows) {
        if let Ok(appdata) = std::env::var("APPDATA") {
            return format!("{appdata}/gossip");
        }
    }
    if let Ok(xdg) = std::env::var("XDG_DATA_HOME") {
        return format!("{xdg}/gossip");
    }
    let home = std::env::var("HOME")
        .or_else(|_| std::env::var("USERPROFILE"))
        .unwrap_or_else(|_| ".".into());
    format!("{home}/.local/share/gossip")
}

struct Args {
    data_dir: String,
    daemon_cmd: Vec<String>,
    name: String,
    import: Option<String>,
    max_attempts: Option<u32>,
}

fn resolve_args() -> Args {
    let mut args = std::env::args().skip(1);
    let mut data_dir = env_or("GOSSIP_DATA_DIR", &default_data_dir());
    let mut daemon = env_or("GOSSIPD", "gossipd");
    let mut name = std::env::var("GOSSIP_NAME").unwrap_or_default();
    let mut import = None;
    let mut max_attempts = std::env::var("GOSSIP_MAX_ATTEMPTS").ok().and_then(|v| v.parse().ok());
    while let Some(a) = args.next() {
        match a.as_str() {
            "--data-dir" => data_dir = args.next().unwrap_or(data_dir),
            "--daemon" => daemon = args.next().unwrap_or(daemon),
            "--name" => name = args.next().unwrap_or(name),
            "--import" => import = args.next(),
            "--max-attempts" => max_attempts = args.next().and_then(|v| v.parse().ok()),
            _ => {}
        }
    }
    Args { data_dir, daemon_cmd: vec![daemon], name, import, max_attempts }
}

const HELP: &str = "\
commands:
  /ticket            print your ticket (share it to be added)
  /add <ticket> [nm] add a contact from their ticket
  /contacts          list contacts (* = online)
  /to <name|id>      set who you are talking to
  /history [n]       show last n messages with current contact
  /file <path>       send a file to current contact
  /downloads         show where received files are saved
  /savedir <path>    change where received files are saved
  /files             show the file-accept policy
  /files accept|reject|ask   set default for all incoming files
  /allow|/block|/ask <name>  per-contact: accept, reject, or ask each file
  /accept|/decline <id>      answer a file you were asked about
  /export <path>     save your whole profile (identity + history) to a file
  /id                show your node id
  /quit              stop the daemon and exit
  <text>             send a chat message to current contact";

fn main() {
    let Args { data_dir, daemon_cmd, name, import, max_attempts } = resolve_args();

    if let Some(archive) = import {
        if std::path::Path::new(&format!("{data_dir}/identity.key")).exists() {
            eprintln!("gossip: {data_dir} already has a profile; import into a fresh --data-dir");
            std::process::exit(1);
        }
        match gossip_client::import_profile(&archive, &data_dir) {
            Ok(node) => println!("gossip: imported profile {node} into {data_dir}"),
            Err(e) => {
                eprintln!("gossip: import failed: {e}");
                std::process::exit(1);
            }
        }
    }

    let control = format!("{data_dir}/gossip.port");
    let (client, notifications) = match Client::connect_or_spawn(&control, &daemon_cmd) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("gossip: cannot reach or start daemon {:?}: {e}", daemon_cmd[0]);
            std::process::exit(1);
        }
    };

    let mut init_params = json!({"data-dir": data_dir, "display-name": name});
    if let Some(n) = max_attempts {
        init_params["backoff"] = json!({"max-attempts": n});
    }
    let init = client
        .request("init", init_params)
        .unwrap_or_else(|e| fatal(&e));
    let node_id = init["node-id"].as_str().unwrap_or("?").to_string();
    println!("gossip: up as {node_id}");
    println!("data-dir: {data_dir}");
    println!("type /help for commands, /ticket to share your address\n");

    std::thread::spawn(move || notification_printer(notifications));

    let mut target: Option<(String, String)> = None;
    let stdin = std::io::stdin();
    prompt(&target);
    for line in stdin.lock().lines() {
        let Ok(line) = line else { break };
        match parse_command(&line) {
            Command::Empty => {}
            Command::Help => println!("{HELP}"),
            Command::Id => println!("{node_id}"),
            Command::Quit => break,
            Command::Ticket => match client.request("contact/makeTicket", json!({})) {
                Ok(v) => println!("{}", v["ticket"].as_str().unwrap_or("?")),
                Err(e) => warn(&e),
            },
            Command::Add(ticket, name) => {
                let params = match name {
                    Some(n) => json!({"ticket": ticket, "name": n}),
                    None => json!({"ticket": ticket}),
                };
                match client.request("contact/addTicket", params) {
                    Ok(v) => {
                        let (id, nm) = (v["id"].as_str().unwrap_or("?"), v["name"].as_str().unwrap_or("?"));
                        println!("added {nm} ({id})");
                        target = Some((id.to_string(), nm.to_string()));
                    }
                    Err(e) => warn(&e),
                }
            }
            Command::Contacts => match client.request("contact/list", json!({})) {
                Ok(v) => print_contacts(&v),
                Err(e) => warn(&e),
            },
            Command::To(who) => match resolve_contact(&client, &who) {
                Some(c) => {
                    println!("talking to {}", c.1);
                    target = Some(c);
                }
                None => warn(&format!("no contact matching {who:?}")),
            },
            Command::History(n) => match &target {
                Some((id, _)) => match client
                    .request("msg/history", json!({"peer-id": id, "limit": n}))
                {
                    Ok(v) => print_history(&v),
                    Err(e) => warn(&e),
                },
                None => warn("set a contact first with /to <name>"),
            },
            Command::Say(body) => match &target {
                Some((id, _)) => {
                    if let Err(e) = client.request("msg/send", json!({"to": id, "body": body})) {
                        warn(&e);
                    }
                }
                None => warn("set a contact first with /to <name>"),
            },
            Command::File(path) => match &target {
                Some((id, _)) => match client.request("blob/send", json!({"to": id, "path": path})) {
                    Ok(_) => println!("sending {path}"),
                    Err(e) => warn(&e),
                },
                None => warn("set a contact first with /to <name>"),
            },
            Command::Downloads => match client.request("status", json!({})) {
                Ok(v) => println!("files are saved to: {}", v["downloads-dir"].as_str().unwrap_or("?")),
                Err(e) => warn(&e),
            },
            Command::SaveDir(path) => {
                match client.request("config/setDownloadsDir", json!({"path": path})) {
                    Ok(v) => println!("files now saved to: {}", v["downloads-dir"].as_str().unwrap_or("?")),
                    Err(e) => warn(&e),
                }
            }
            Command::Files(rest) => handle_files(&client, rest.trim()),
            Command::Allow(name) => set_contact_policy(&client, &name, "accept"),
            Command::Block(name) => set_contact_policy(&client, &name, "reject"),
            Command::Ask(name) => set_contact_policy(&client, &name, "ask"),
            Command::Accept(id) => respond_file(&client, &id, true),
            Command::Decline(id) => respond_file(&client, &id, false),
            Command::Export(path) if path.is_empty() => warn("usage: /export <path>"),
            Command::Export(path) => match client.request("profile/export", json!({"path": path})) {
                Ok(v) => println!("exported profile to {}", v["path"].as_str().unwrap_or(&path)),
                Err(e) => warn(&e),
            },
            Command::Unknown(c) => warn(&format!("unknown command {c} (try /help)")),
        }
        prompt(&target);
    }

    client.request("shutdown", json!({})).ok();
}

fn handle_files(client: &Client, arg: &str) {
    if arg.is_empty() {
        match client.request("status", json!({})) {
            Ok(v) => println!("files: default={}", v["files"]["default"].as_str().unwrap_or("?")),
            Err(e) => warn(&e),
        }
        return;
    }
    if !matches!(arg, "accept" | "reject" | "ask") {
        return warn("usage: /files [accept|reject|ask]");
    }
    match client.request("config/setFilePolicy", json!({"default": arg})) {
        Ok(v) => println!("file policy: default={}", v["default"].as_str().unwrap_or("?")),
        Err(e) => warn(&e),
    }
}

fn respond_file(client: &Client, id: &str, accept: bool) {
    match client.request("file/respond", json!({"id": id, "accept": accept})) {
        Ok(_) => println!("{} {id}", if accept { "accepted" } else { "declined" }),
        Err(e) => warn(&e),
    }
}

fn set_contact_policy(client: &Client, name: &str, policy: &str) {
    match resolve_contact(client, name) {
        Some((id, nm)) => {
            match client.request("config/setContactFilePolicy", json!({"id": id, "policy": policy})) {
                Ok(_) => println!("files from {nm}: {policy}"),
                Err(e) => warn(&e),
            }
        }
        None => warn(&format!("no contact matching {name:?}")),
    }
}

fn resolve_contact(client: &Client, who: &str) -> Option<(String, String)> {
    let list = client.request("contact/list", json!({})).ok()?;
    let who_l = who.to_lowercase();
    list.as_array()?.iter().find_map(|c| {
        let id = c["id"].as_str()?;
        let name = c["name"].as_str()?;
        let hit = name.eq_ignore_ascii_case(who) || id == who || id.starts_with(&who_l);
        hit.then(|| (id.to_string(), name.to_string()))
    })
}

fn print_contacts(v: &Value) {
    let Some(list) = v.as_array() else { return };
    if list.is_empty() {
        println!("no contacts yet, share /ticket or /add one");
        return;
    }
    for c in list {
        let dot = if c["online"].as_bool() == Some(true) { "*" } else { " " };
        println!("{dot} {}  {}", c["name"].as_str().unwrap_or("?"), c["id"].as_str().unwrap_or("?"));
    }
}

fn print_history(v: &Value) {
    let Some(list) = v.as_array() else { return };
    for m in list {
        let who = m["from-name"].as_str().unwrap_or("?");
        let ts = m["ts"].as_f64().unwrap_or(0.0);
        println!("[{}] {who}: {}", hhmm(ts), body_text(m));
    }
}

fn body_text(m: &Value) -> String {
    let body = m["body"].as_str().unwrap_or("");
    if m["kind"].as_str() == Some("file") {
        format!("[file] {body}")
    } else {
        body.to_string()
    }
}

fn notification_printer(rx: std::sync::mpsc::Receiver<Notification>) {
    for n in rx {
        match n.method.as_str() {
            "msg/received" => {
                let who = n.params["from-name"].as_str().unwrap_or("?");
                let ts = n.params["ts"].as_f64().unwrap_or(0.0);
                println!("\n[{}] {who}: {}", hhmm(ts), body_text(&n.params));
            }
            "msg/sent" => {
                let to = n.params["to-name"].as_str().unwrap_or("?");
                let ts = n.params["ts"].as_f64().unwrap_or(0.0);
                println!("\n[{}] you -> {to}: {}", hhmm(ts), body_text(&n.params));
            }
            "log" => {
                if let Some(m) = n.params["message"].as_str() {
                    println!("\n* {m}");
                }
            }
            "transfer/progress" if n.params["percent"].as_i64() == Some(100) => {
                println!("\n* file sent");
            }
            "file/incoming" => {
                let (who, name, id) = (
                    n.params["from-name"].as_str().unwrap_or("?"),
                    n.params["name"].as_str().unwrap_or("?"),
                    n.params["id"].as_str().unwrap_or("?"),
                );
                println!(
                    "\n* {who} wants to send you '{name}' ({} bytes) - /accept {id} or /decline {id}",
                    n.params["size"].as_u64().unwrap_or(0)
                );
            }
            "file/declined" => {
                println!(
                    "\n· declined file {} from {}",
                    n.params["name"].as_str().unwrap_or("?"),
                    n.params["from-name"].as_str().unwrap_or("?")
                );
            }
            _ => {}
        }
        std::io::stdout().flush().ok();
    }
}

fn prompt(target: &Option<(String, String)>) {
    match target {
        Some((_, name)) => print!("{name}> "),
        None => print!("(no contact)> "),
    }
    std::io::stdout().flush().ok();
}

fn warn(msg: &str) {
    println!("gossip: {msg}");
}

fn fatal(msg: &str) -> Value {
    eprintln!("gossip: {msg}");
    std::process::exit(1);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_commands_and_plain_text() {
        assert!(matches!(parse_command("  "), Command::Empty));
        assert!(matches!(parse_command("hello there"), Command::Say(s) if s == "hello there"));
        assert!(matches!(parse_command("/to bob"), Command::To(s) if s == "bob"));
        assert!(matches!(parse_command("/history"), Command::History(20)));
        assert!(matches!(parse_command("/history 5"), Command::History(5)));
        match parse_command("/add gossip:abc alice") {
            Command::Add(t, Some(n)) => {
                assert_eq!(t, "gossip:abc");
                assert_eq!(n, "alice");
            }
            _ => panic!("expected add with name"),
        }
        assert!(matches!(parse_command("/add gossip:abc"), Command::Add(_, None)));
        assert!(matches!(parse_command("/nope"), Command::Unknown(_)));
    }
}
