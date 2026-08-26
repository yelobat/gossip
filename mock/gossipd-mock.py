#!/usr/bin/env python3
"""gossipd-mock — protocol double for gossip.el.

Speaks JSON-RPC 2.0 over stdio with LSP-style Content-Length framing
(what Emacs's jsonrpc.el expects from a process connection).

Simulated world:
  * contact "alice" is online: sends get an immediate delivery ack and
    a canned reply ~1.5s later.
  * contact "bob" is OFFLINE: sends are queued and redelivery is
    attempted with real exponential backoff (initial * multiplier^n,
    plus jitter, capped at max — all taken from the `init` params, so
    the values you customize in Emacs are the values you watch here).
    After MAX_ATTEMPTS failed dials bob "comes back online": presence
    flips, the queued message is delivered, and bob replies.

Everything nondeterministic runs on daemon threads; stdout writes are
serialized with a lock. Logs go to stderr only.
"""

import json
import random
import sys
import threading
import time

MAX_ATTEMPTS = 4  # bob reappears on this dial attempt

write_lock = threading.Lock()
state_lock = threading.Lock()

state = {
    "node_id": "gsp1-luk-mock-3f9c1e",
    "display_name": "luk",
    "backoff": {"initial-seconds": 1.0, "max-seconds": 300.0,
                "multiplier": 2.0, "jitter": 0.2},
    "transport": {"allow-relays": False, "relay-urls": [],
                  "tor": {"enabled": False}, "advertised-addrs": []},
    "tor_state": {"bootstrapped": False,
                  "onion": "gspluk7h2v3m4x5tqe6r7y8u9i0o1p2a3s4d5f6g7h8j9k0l1z2x3c4v5b6n7m8.onion"},
    "inbound_direct": None,
    "contacts": {
        "gsp1-alice-77aa": {"id": "gsp1-alice-77aa", "name": "alice",
                            "online": True},
        "gsp1-bob-42dd": {"id": "gsp1-bob-42dd", "name": "bob",
                          "online": False},
        # carol is up, but behind a hard NAT: in a direct-only world we
        # can never dial HER — she has to dial US.
        "gsp1-carol-9e1f": {"id": "gsp1-carol-9e1f", "name": "carol",
                            "online": True, "reachable": False},
        # dave is hard-NATed like carol, but runs the tor transport:
        # reachable if and only if our tor side is up.
        "gsp1-dave-c4a2": {"id": "gsp1-dave-c4a2", "name": "dave",
                           "online": True, "reachable": False,
                           "tor": True,
                           "onion": "gspdave1qw2er3ty4ui5op6as7df8gh9jk0lz1xc2vb3nm4qw5er6ty7ui8op9as0d.onion"},
    },
    "history": {},   # peer_id -> [msg, ...]
    "queue": {},     # msg_id -> {"to":..., "attempts":..., "next_at":...}
    "msg_seq": 0,
}


def log(text):
    print(f"gossipd-mock: {text}", file=sys.stderr, flush=True)


def write_frame(payload):
    body = json.dumps(payload)
    data = f"Content-Length: {len(body.encode('utf-8'))}\r\n\r\n{body}"
    with write_lock:
        sys.stdout.write(data)
        sys.stdout.flush()


def notify(method, params):
    write_frame({"jsonrpc": "2.0", "method": method, "params": params})


def respond(req_id, result):
    write_frame({"jsonrpc": "2.0", "id": req_id, "result": result})


def respond_error(req_id, code, message):
    write_frame({"jsonrpc": "2.0", "id": req_id,
                 "error": {"code": code, "message": message}})


def read_frame(stream):
    length = None
    while True:
        line = stream.readline()
        if not line:
            return None
        line = line.strip()
        if not line:
            break
        if line.lower().startswith(b"content-length:"):
            length = int(line.split(b":", 1)[1])
    if length is None:
        return None
    return json.loads(stream.read(length).decode("utf-8"))


def next_msg_id():
    with state_lock:
        state["msg_seq"] += 1
        return f"m{state['msg_seq']:04d}"


def record(peer_id, msg):
    with state_lock:
        state["history"].setdefault(peer_id, []).append(msg)


def backoff_delay(attempts):
    cfg = state["backoff"]
    delay = min(cfg["initial-seconds"] * (cfg["multiplier"] ** attempts),
                cfg["max-seconds"])
    return delay * (1.0 + random.uniform(-cfg["jitter"], cfg["jitter"]))


def alice_flow(msg_id, peer_id, body):
    time.sleep(0.6)
    notify("msg/delivered", {"msg-id": msg_id, "to": peer_id,
                             "ts": time.time()})
    time.sleep(0.9)
    reply = {"id": next_msg_id(), "from": peer_id, "from-name": "alice",
             "kind": "chat", "body": f"alice here — got \"{body[:40]}\"",
             "ts": time.time()}
    record(peer_id, reply)
    notify("msg/received", reply)


def bob_retry_loop(msg_id, peer_id, body):
    """Redial bob with exponential backoff until he 'comes back'."""
    attempts = 0
    while True:
        delay = backoff_delay(attempts)
        attempts += 1
        with state_lock:
            state["queue"][msg_id] = {"to": peer_id, "attempts": attempts,
                                      "next_at": time.time() + delay}
        notify("queue/update", {"msg-id": msg_id, "to": peer_id,
                                "to-name": "bob", "attempts": attempts,
                                "delay-seconds": round(delay, 2)})
        log(f"dial bob attempt {attempts} failed; retry in {delay:.2f}s")
        time.sleep(delay)
        if attempts >= MAX_ATTEMPTS:
            break
    # bob is back: presence, delivery, reply
    with state_lock:
        state["contacts"][peer_id]["online"] = True
        state["queue"].pop(msg_id, None)
    notify("peer/presence", {"peer-id": peer_id, "online": True})
    notify("msg/delivered", {"msg-id": msg_id, "to": peer_id,
                             "ts": time.time()})
    reply = {"id": next_msg_id(), "from": peer_id, "from-name": "bob",
             "kind": "chat",
             "body": f"back online — laptop was closed. got \"{body[:40]}\"",
             "ts": time.time()}
    record(peer_id, reply)
    notify("msg/received", reply)


def carol_retry_loop(msg_id, peer_id, body):
    """Direct-only failure mode: outbound dials to a hard-NATed peer
    never succeed.  Delivery happens only when carol dials US and the
    log syncs — unidirectional reachability is enough."""
    attempts = 0
    while attempts < MAX_ATTEMPTS + 1:
        delay = backoff_delay(attempts)
        attempts += 1
        with state_lock:
            state["queue"][msg_id] = {"to": peer_id, "attempts": attempts,
                                      "next_at": time.time() + delay}
        notify("queue/update", {"msg-id": msg_id, "to": peer_id,
                                "to-name": "carol", "attempts": attempts,
                                "delay-seconds": round(delay, 2)})
        time.sleep(delay)
    notify("log", {"level": "warn",
                   "message": "no direct path to carol (hard NAT); "
                              "parking at max backoff — she can still "
                              "dial us"})
    time.sleep(1.5)  # ...and eventually she does
    with state_lock:
        state["queue"].pop(msg_id, None)
    notify("peer/presence", {"peer-id": peer_id, "online": True,
                             "path": "direct-inbound"})
    notify("msg/delivered", {"msg-id": msg_id, "to": peer_id,
                             "ts": time.time()})
    reply = {"id": next_msg_id(), "from": peer_id, "from-name": "carol",
             "kind": "chat",
             "body": "connected to you directly — you'll never reach me "
                     "behind this NAT, so I do the dialing",
             "ts": time.time()}
    record(peer_id, reply)
    notify("msg/received", reply)


def tor_bootstrap():
    for state_name, percent in (("connecting", 25), ("handshaking", 60),
                                ("done", 100)):
        time.sleep(0.4)
        notify("tor/status", {"state": state_name, "percent": percent})
    with state_lock:
        state["tor_state"]["bootstrapped"] = True
    log(f"tor bootstrapped; onion {state['tor_state']['onion']}")


def dave_tor_flow(msg_id, peer_id, body):
    """Tor path: wait for bootstrap, then deliver with tor latency."""
    while True:
        with state_lock:
            if state["tor_state"]["bootstrapped"]:
                break
        time.sleep(0.2)
    time.sleep(1.2)  # onion connect + RTT
    notify("msg/delivered", {"msg-id": msg_id, "to": peer_id,
                             "path": "tor", "ts": time.time()})
    time.sleep(1.0)
    reply = {"id": next_msg_id(), "from": peer_id, "from-name": "dave",
             "kind": "chat",
             "body": "reading you over my onion — no idea what your IP "
                     "is, and you don't know mine",
             "ts": time.time()}
    record(peer_id, reply)
    notify("msg/received", reply)


def blob_flow(transfer_id):
    for percent in (33, 67, 100):
        time.sleep(0.4)
        notify("transfer/progress", {"transfer-id": transfer_id,
                                     "percent": percent})


def handle(request):
    method = request.get("method")
    params = request.get("params") or {}
    req_id = request.get("id")

    if method == "init":
        if isinstance(params.get("backoff"), dict):
            state["backoff"].update(params["backoff"])
        if isinstance(params.get("transport"), dict):
            state["transport"].update(params["transport"])
        if params.get("display-name"):
            state["display_name"] = params["display-name"]
        log(f"init: backoff={state['backoff']} "
            f"transport={state['transport']}")
        if (state["transport"].get("tor") or {}).get("enabled"):
            threading.Thread(target=tor_bootstrap, daemon=True).start()
        respond(req_id, {"node-id": state["node_id"],
                         "display-name": state["display_name"]})

    elif method == "identity/get":
        respond(req_id, {"node-id": state["node_id"],
                         "display-name": state["display_name"]})

    elif method == "identity/setName":
        state["display_name"] = params.get("name", state["display_name"])
        respond(req_id, {"ok": True})

    elif method == "contact/list":
        with state_lock:
            respond(req_id, list(state["contacts"].values()))

    elif method == "contact/makeTicket":
        respond(req_id, {"ticket": f"gossip:{state['node_id']}"
                                   ":relay=euw1.example:topic=x9k2"})

    elif method == "contact/addTicket":
        ticket = params.get("ticket", "")
        if not ticket.startswith("gossip:"):
            respond_error(req_id, -32602, "not a gossip ticket")
            return
        peer_id = ticket.split(":")[1] or f"gsp1-anon-{random.randint(0,999)}"
        name = params.get("name") or peer_id[:12]
        contact = {"id": peer_id, "name": name, "online": True}
        with state_lock:
            state["contacts"][peer_id] = contact
        respond(req_id, contact)

    elif method == "msg/send":
        peer_id = params["to"]
        body = params.get("body", "")
        msg_id = next_msg_id()
        with state_lock:
            contact = state["contacts"].get(peer_id)
        if contact is None:
            respond_error(req_id, -32602, f"unknown recipient {peer_id}")
            return
        record(peer_id, {"id": msg_id, "from": state["node_id"],
                         "from-name": state["display_name"],
                         "kind": params.get("kind", "chat"),
                         "body": body, "ts": time.time()})
        if not contact["online"]:
            respond(req_id, {"msg-id": msg_id, "status": "queued"})
            threading.Thread(target=bob_retry_loop,
                             args=(msg_id, peer_id, body),
                             daemon=True).start()
        elif (not contact.get("reachable", True) and contact.get("tor")
              and (state["transport"].get("tor") or {}).get("enabled")):
            respond(req_id, {"msg-id": msg_id, "status": "sent",
                             "path": "tor"})
            threading.Thread(target=dave_tor_flow,
                             args=(msg_id, peer_id, body),
                             daemon=True).start()
        elif not contact.get("reachable", True):
            respond(req_id, {"msg-id": msg_id, "status": "queued"})
            threading.Thread(target=carol_retry_loop,
                             args=(msg_id, peer_id, body),
                             daemon=True).start()
        else:
            respond(req_id, {"msg-id": msg_id, "status": "sent"})
            threading.Thread(target=alice_flow,
                             args=(msg_id, peer_id, body),
                             daemon=True).start()

    elif method == "msg/history":
        with state_lock:
            history = list(state["history"].get(params.get("peer-id"), []))
        respond(req_id, history[-int(params.get("limit", 100)):])

    elif method == "blob/send":
        transfer_id = f"t{random.randint(1000, 9999)}"
        respond(req_id, {"transfer-id": transfer_id})
        threading.Thread(target=blob_flow, args=(transfer_id,),
                         daemon=True).start()

    elif method == "net/check":
        # Dial-back probe via a connected friend. The mock simulates a
        # VPN'd / NATed host: outbound fine, inbound direct impossible.
        with state_lock:
            state["inbound_direct"] = False
        notify("log", {"level": "info",
                       "message": "dial-back via alice: your direct "
                                  "addresses are not reachable from "
                                  "outside (VPN/NAT) — dial-out-only "
                                  "mode"})
        respond(req_id, {"inbound-direct": False, "checked-via": "alice"})

    elif method == "status":
        now = time.time()
        with state_lock:
            queue = [{"msg-id": mid, "to-name":
                      state["contacts"][q["to"]]["name"],
                      "attempts": q["attempts"],
                      "next-in-seconds": round(max(0.0, q["next_at"] - now), 1)}
                     for mid, q in state["queue"].items()]
            contacts = list(state["contacts"].values())
        transport = state["transport"]
        relay = ("disabled — direct connections only"
                 if not transport["allow-relays"] or not transport["relay-urls"]
                 else ", ".join(transport["relay-urls"]) + " (self-hosted)")
        with state_lock:
            tor_cfg = (state["transport"].get("tor") or {})
            if not tor_cfg.get("enabled"):
                tor = "off"
            elif not state["tor_state"]["bootstrapped"]:
                tor = "bootstrapping"
            else:
                tor = state["tor_state"]["onion"]
            inbound = state["inbound_direct"]
        respond(req_id, {"node-id": state["node_id"], "online": True,
                         "relay": relay, "tor": tor,
                         "inbound-direct": ("unchecked" if inbound is None
                                           else inbound),
                         "advertised-addrs":
                             state["transport"].get("advertised-addrs", []),
                         "contacts": contacts, "queue": queue})

    elif method == "shutdown":
        respond(req_id, {"ok": True})
        log("shutdown")
        sys.exit(0)

    else:
        if req_id is not None:
            respond_error(req_id, -32601, f"unknown method {method}")


def main():
    log("mock daemon up; alice online, bob offline")
    stream = sys.stdin.buffer
    while True:
        request = read_frame(stream)
        if request is None:
            log("stdin closed; exiting")
            return
        try:
            handle(request)
        except Exception as exc:  # keep the loop alive for the workshop
            log(f"error handling {request.get('method')}: {exc}")
            if request.get("id") is not None:
                respond_error(request["id"], -32603, str(exc))


if __name__ == "__main__":
    main()
