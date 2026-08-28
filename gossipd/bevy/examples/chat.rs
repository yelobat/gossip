use std::sync::Mutex;
use std::sync::mpsc::Receiver;
use std::time::Duration;

use bevy::app::ScheduleRunnerPlugin;
use bevy::prelude::*;
use gossip_bevy::{Gossip, GossipEvent, GossipPlugin};

#[derive(Resource)]
struct Stdin(Mutex<Receiver<String>>);

#[derive(Resource, Default)]
struct LastPeer(Option<String>);

fn main() {
    let (tx, rx) = std::sync::mpsc::channel::<String>();
    std::thread::spawn(move || {
        let stdin = std::io::stdin();
        let mut line = String::new();
        loop {
            line.clear();
            match stdin.read_line(&mut line) {
                Ok(0) | Err(_) => break,
                Ok(_) => {
                    if tx.send(line.trim().to_string()).is_err() {
                        break;
                    }
                }
            }
        }
    });

    App::new()
        .add_plugins(
            MinimalPlugins.set(ScheduleRunnerPlugin::run_loop(Duration::from_millis(50))),
        )
        .add_plugins(GossipPlugin::default())
        .insert_resource(Stdin(Mutex::new(rx)))
        .init_resource::<LastPeer>()
        .add_systems(Startup, banner)
        .add_systems(Update, (print_events, send_from_stdin))
        .run();
}

fn banner(gossip: Res<Gossip>) {
    println!("gossip-bevy demo up as {}", gossip.node_id());
    println!("type to reply · /ticket · /add <ticket> <name> · /file <path>");
    gossip.make_ticket();
}

fn label(kind: &str, body: &str) -> String {
    if kind == "file" {
        format!("[file] {body}")
    } else {
        body.to_string()
    }
}

fn print_events(mut events: MessageReader<GossipEvent>, mut peer: ResMut<LastPeer>) {
    for e in events.read() {
        match e {
            GossipEvent::Received { from, from_name, kind, body, .. } => {
                println!("<< {from_name}: {}", label(kind, body));
                peer.0 = Some(from.clone());
            }
            GossipEvent::Sent { to_name, kind, body, .. } => {
                println!(">> you -> {to_name}: {}", label(kind, body));
            }
            GossipEvent::Delivered { to, .. } => println!("   (delivered to {to})"),
            GossipEvent::FileOffered { id, from_name, name, size, .. } => println!(
                "   {from_name} offers '{name}' ({size} bytes) - /accept {id} or /decline {id}"
            ),
            GossipEvent::FileDeclined { from_name, name, .. } => {
                println!("   declined file {name} from {from_name}")
            }
            GossipEvent::ContactAdded { id, name } => {
                println!("   added contact {name}");
                peer.0 = Some(id.clone());
            }
            GossipEvent::Ticket(t) => println!("   my ticket: {t}"),
            GossipEvent::Log { message } => println!("   * {message}"),
            GossipEvent::Error(e) => println!("   ! error: {e}"),
        }
    }
}

fn send_from_stdin(stdin: Res<Stdin>, gossip: Res<Gossip>, peer: Res<LastPeer>) {
    let Ok(rx) = stdin.0.lock() else { return };
    while let Ok(line) = rx.try_recv() {
        if line.is_empty() {
            continue;
        }
        if line == "/ticket" {
            gossip.make_ticket();
        } else if let Some(id) = line.strip_prefix("/accept ") {
            gossip.respond_file(id.trim(), true);
        } else if let Some(id) = line.strip_prefix("/decline ") {
            gossip.respond_file(id.trim(), false);
        } else if let Some(rest) = line.strip_prefix("/add ") {
            let mut it = rest.splitn(2, char::is_whitespace);
            let ticket = it.next().unwrap_or("");
            let name = it.next().map(str::trim).filter(|n| !n.is_empty()).unwrap_or("peer");
            gossip.add_contact(ticket, name);
        } else if let Some(path) = line.strip_prefix("/file ") {
            match &peer.0 {
                Some(id) => gossip.send_file(id.clone(), path.trim().to_string()),
                None => println!("   ! no peer yet — wait for a message or /add a ticket"),
            }
        } else {
            match &peer.0 {
                Some(id) => gossip.send(id.clone(), line),
                None => println!("   ! no peer yet — wait for a message or /add a ticket"),
            }
        }
    }
}
