"""
node.py — Network Simulator Node Agent  v4.0
============================================
New in this version:
  1. Heartbeat   — background thread pings every known neighbor every
                   HEARTBEAT_INTERVAL seconds and automatically transitions
                   status:  up → degraded (1 miss) → down (2+ misses).
                   Status recovers to up as soon as a ping succeeds again.

  3. ACK + retransmit — every routed /message gets an ACK sent back along
                        the reverse path. The sender keeps a pending-ACK
                        table; if no ACK arrives within ACK_TIMEOUT seconds
                        it retransmits up to MAX_RETRIES times, then marks
                        the message dropped.

  4. HELLO self-announcement — on startup the node floods a HELLO message
                        containing its ID and capabilities so that peer nodes
                        can discover it without being told explicitly.
                        HELLO is re-flooded whenever the neighbor table grows.

  5. Gossip anti-entropy — a background thread wakes every GOSSIP_INTERVAL
                        seconds, picks a random live neighbor, exchanges store
                        contents, and merges — no orchestrator involvement.
"""
import json, os, time, random, threading, urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer
from collections import deque

# ── Configuration ─────────────────────────────────────────────────────────
NODE_ID            = os.environ["NODE_ID"]
NODE_PORT          = int(os.environ.get("NODE_PORT",          8000))
HEARTBEAT_INTERVAL = float(os.environ.get("HEARTBEAT_INTERVAL", 5))
GOSSIP_INTERVAL    = float(os.environ.get("GOSSIP_INTERVAL",    8))
ACK_TIMEOUT        = float(os.environ.get("ACK_TIMEOUT",        3))
MAX_RETRIES        = int(os.environ.get("MAX_RETRIES",          2))

# ── Shared state ──────────────────────────────────────────────────────────
neighbors: dict        = {}
neighbors_lock         = threading.Lock()

vector_clock: dict     = {NODE_ID: 0}
vc_lock                = threading.Lock()

recent_messages: deque = deque(maxlen=40)

flood_seen: set        = set()
flood_lock             = threading.Lock()

data_store: dict       = {}
store_lock             = threading.Lock()

# ACK pending:  msg_id -> {event, retries, path, data, ts}
ack_pending: dict      = {}
ack_lock               = threading.Lock()

# ─────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────

def _default_metrics() -> dict:
    return {
        "weight": 1, "bandwidth": 100, "delay": 5.0,
        "jitter": 1.0, "speed": 100, "loss": 0.0,
        "status": "up", "mtu": 1500, "_miss_count": 0,
    }

def _log(action: str, detail: str = ""):
    recent_messages.append({"id": action, "action": f"{action} {detail}".strip(), "ts": time.time()})
    print(f"[{NODE_ID}] {action} {detail}", flush=True)

def send_json(handler, body, code=200):
    raw = json.dumps(body).encode()
    handler.send_response(code)
    handler.send_header("Content-Type",   "application/json")
    handler.send_header("Content-Length", str(len(raw)))
    handler.end_headers()
    handler.wfile.write(raw)

def vc_increment():
    with vc_lock:
        vector_clock[NODE_ID] = vector_clock.get(NODE_ID, 0) + 1

def vc_merge(incoming: dict):
    with vc_lock:
        for k, v in incoming.items():
            vector_clock[k] = max(vector_clock.get(k, 0), v)

def vc_snapshot() -> dict:
    with vc_lock:
        return dict(vector_clock)

def _peer_url(nid: str, path: str) -> str:
    return f"http://node_{nid.lower()}:{NODE_PORT}{path}"

def ping_node(nid: str) -> bool:
    try:
        with urllib.request.urlopen(_peer_url(nid, "/ping"), timeout=1) as r:
            return r.status == 200
    except Exception:
        return False

def post_json(url: str, body: dict, timeout: float = 2) -> dict:
    raw = json.dumps(body).encode()
    req = urllib.request.Request(url, data=raw, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())

def get_json(url: str, timeout: float = 2) -> dict:
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.loads(r.read())

# ─────────────────────────────────────────────────────────────────────────
# 1. HEARTBEAT
# ─────────────────────────────────────────────────────────────────────────

def _heartbeat_loop():
    """
    Probe every known neighbor on a fixed interval.
    Miss count drives automatic status transitions:
      0 misses -> up | 1 miss -> degraded | 2+ misses -> down
    First successful ping after a failure resets to up immediately.
    """
    time.sleep(2)   # let the HTTP server bind first
    while True:
        time.sleep(HEARTBEAT_INTERVAL)
        with neighbors_lock:
            nids = list(neighbors.keys())

        for nid in nids:
            alive = ping_node(nid)
            with neighbors_lock:
                if nid not in neighbors:
                    continue
                m = neighbors[nid]
                if alive:
                    if m["_miss_count"] > 0:
                        _log("heartbeat_recovered", f"neighbor={nid} -> up")
                    m["_miss_count"] = 0
                    m["status"]      = "up"
                else:
                    m["_miss_count"] += 1
                    if m["_miss_count"] == 1:
                        m["status"] = "degraded"
                        _log("heartbeat_degraded", f"neighbor={nid} miss=1")
                    else:
                        m["status"] = "down"
                        _log("heartbeat_down", f"neighbor={nid} miss={m['_miss_count']}")

# ─────────────────────────────────────────────────────────────────────────
# 3. ACK + RETRANSMIT
# ─────────────────────────────────────────────────────────────────────────

def _send_with_ack(envelope: dict):
    """
    Forward envelope to the next hop and wait for an ACK.
    If no ACK arrives within ACK_TIMEOUT, retransmit up to MAX_RETRIES times.
    Each attempt runs in a daemon thread to avoid blocking the HTTP handler.
    """
    msg_id = envelope["msg_id"]
    path   = envelope["path"]
    try:
        my_idx = path.index(NODE_ID)
    except ValueError:
        return
    if my_idx + 1 >= len(path):
        return   # we are the final destination

    nxt = path[my_idx + 1]
    url = _peer_url(nxt, "/message")

    evt = threading.Event()
    with ack_lock:
        ack_pending[msg_id] = {"event": evt, "retries": 0,
                                "path": path, "data": envelope,
                                "ts": time.time()}

    def _try_send():
        for attempt in range(MAX_RETRIES + 1):
            try:
                post_json(url, envelope)
                recent_messages.append({"id": msg_id,
                    "action": f"forwarded_to_{nxt} attempt={attempt+1}",
                    "ts": time.time()})
            except Exception as e:
                recent_messages.append({"id": msg_id,
                    "action": f"send_failed_to_{nxt} err={e}",
                    "ts": time.time()})

            if evt.wait(timeout=ACK_TIMEOUT):
                with ack_lock:
                    ack_pending.pop(msg_id, None)
                _log(f"ack_ok", f"msg={msg_id} attempt={attempt+1}")
                return

            if attempt < MAX_RETRIES:
                _log("ack_timeout", f"msg={msg_id} retrying attempt={attempt+2}")
            else:
                _log("ack_failed", f"msg={msg_id} dropped after {MAX_RETRIES+1} attempts")
                with ack_lock:
                    ack_pending.pop(msg_id, None)
                recent_messages.append({"id": msg_id,
                    "action": "dropped_no_ack", "ts": time.time()})

    threading.Thread(target=_try_send, daemon=True).start()


def _send_ack(msg_id: str, path: list):
    """Send a single-hop ACK back to our predecessor on the path."""
    try:
        my_idx = path.index(NODE_ID)
    except ValueError:
        return
    if my_idx == 0:
        return   # we are the source — nothing upstream to ACK
    prev = path[my_idx - 1]
    try:
        post_json(_peer_url(prev, "/ack"), {"msg_id": msg_id}, timeout=1)
    except Exception:
        pass   # best-effort

# ─────────────────────────────────────────────────────────────────────────
# 4. HELLO self-announcement
# ─────────────────────────────────────────────────────────────────────────

def _broadcast_hello():
    """
    Flood a HELLO to all live non-down neighbors.
    Called at startup and whenever a new neighbor is discovered.
    """
    hello_id = f"hello_{NODE_ID}_{int(time.time())}"
    payload  = json.dumps({
        "type":         "HELLO",
        "from":         NODE_ID,
        "capabilities": ["routing", "flooding", "gossip", "ack"],
        "port":         NODE_PORT,
    })
    uid = f"HELLO:{hello_id}"
    with flood_lock:
        flood_seen.add(uid)

    vc_increment()

    with neighbors_lock:
        nids = [nid for nid, m in neighbors.items() if m.get("status") != "down"]

    _log("hello_sent", f"to {nids}")
    for nid in nids:
        if ping_node(nid):
            try:
                post_json(_peer_url(nid, "/flood"), {
                    "origin":  NODE_ID,
                    "msg_id":  hello_id,
                    "payload": payload,
                    "ttl":     8,
                })
            except Exception:
                pass

# ─────────────────────────────────────────────────────────────────────────
# 5. GOSSIP ANTI-ENTROPY
# ─────────────────────────────────────────────────────────────────────────

def _gossip_loop():
    """
    Every GOSSIP_INTERVAL seconds:
      - Pick a random live neighbor
      - Pull their store (GET /store/get)
      - Merge using VC-sum LWW
      - Push our merged store back (POST /store/sync)
    Converges all nodes without orchestrator involvement.
    """
    time.sleep(GOSSIP_INTERVAL)
    while True:
        time.sleep(GOSSIP_INTERVAL)
        with neighbors_lock:
            candidates = [nid for nid, m in neighbors.items()
                          if m.get("status") != "down"]

        live = [nid for nid in candidates if ping_node(nid)]
        if not live:
            continue

        peer = random.choice(live)
        try:
            resp       = get_json(_peer_url(peer, "/store/get"))
            peer_store = resp.get("store", {})
            peer_vc    = resp.get("vc",    {})

            updates = 0
            with store_lock:
                for key, entry in peer_store.items():
                    local = data_store.get(key)
                    if local is None:
                        data_store[key] = entry
                        updates += 1
                    else:
                        if sum(entry.get("vc", {}).values()) > \
                           sum(local.get("vc",  {}).values()):
                            data_store[key] = entry
                            updates += 1

            vc_merge(peer_vc)

            with store_lock:
                my_store = dict(data_store)

            post_json(_peer_url(peer, "/store/sync"),
                      {"store": my_store, "vc": vc_snapshot()})

            if updates:
                _log("gossip_sync", f"peer={peer} imported={updates} keys")

        except Exception as exc:
            _log("gossip_error", f"peer={peer} err={exc}")

# ─────────────────────────────────────────────────────────────────────────
# HTTP Handler
# ─────────────────────────────────────────────────────────────────────────

class Handler(BaseHTTPRequestHandler):

    def do_GET(self):
        if self.path == "/ping":
            send_json(self, {"id": NODE_ID, "ok": True})

        elif self.path == "/status":
            with neighbors_lock:
                nbrs = dict(neighbors)
            with store_lock:
                keys = list(data_store.keys())
            send_json(self, {
                "id":          NODE_ID,
                "vc":          vc_snapshot(),
                "store_keys":  keys,
                "neighbors":   nbrs,
                "ack_pending": list(ack_pending.keys()),
            })

        elif self.path == "/metrics":
            with neighbors_lock:
                rows = [{"from": NODE_ID, "to": nid, **m}
                        for nid, m in neighbors.items()]
            send_json(self, {"id": NODE_ID, "links": rows})

        elif self.path == "/traffic":
            send_json(self, {"id": NODE_ID, "recent": list(recent_messages)})

        else:
            self.send_error(404)

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        data   = json.loads(self.rfile.read(length) or b"{}")

        # /neighbors
        if self.path == "/neighbors":
            changed = False
            for nid, raw in data.get("neighbors", {}).items():
                base = _default_metrics()
                base.update({k: raw[k] for k in raw if k in base})
                base["weight"]    = int(base["weight"])
                base["bandwidth"] = int(base["bandwidth"])
                base["delay"]     = float(base["delay"])
                base["jitter"]    = float(base["jitter"])
                base["speed"]     = int(base["speed"])
                base["loss"]      = float(base["loss"])
                base["mtu"]       = int(base["mtu"])
                with neighbors_lock:
                    if nid not in neighbors:
                        changed = True
                    neighbors[nid] = base
            send_json(self, {"set": list(neighbors.keys())})
            if changed:
                threading.Thread(target=_broadcast_hello, daemon=True).start()

        # /reach
        elif self.path == "/reach":
            with neighbors_lock:
                nids = list(neighbors.keys())
            live = [nid for nid in nids if ping_node(nid)]
            with neighbors_lock:
                m_snap = dict(neighbors)
            send_json(self, {"id": NODE_ID, "live": live, "metrics": m_snap})

        # /message — receive + forward with ACK
        elif self.path == "/message":
            msg_id  = data.get("msg_id", "unknown")
            path    = data.get("path",   [])
            is_src  = path and path[0]  == NODE_ID
            is_dest = path and path[-1] == NODE_ID
            vc_increment()

            action = "generated" if is_src else "received"
            recent_messages.append({"id": msg_id, "action": action,
                                     "ts": time.time()})

            # ACK back to previous hop as soon as we receive
            if not is_src:
                _send_ack(msg_id, path)

            # Forward if we are not the final destination
            if not is_dest:
                _send_with_ack(data)

            send_json(self, {"status": "ok"})

        # /ack — downstream node confirmed delivery
        elif self.path == "/ack":
            msg_id = data.get("msg_id", "")
            with ack_lock:
                entry = ack_pending.get(msg_id)
            if entry:
                entry["event"].set()
                _log("ack_received", f"msg={msg_id}")
            send_json(self, {"status": "ok"})

        # /flood — with TTL + HELLO parsing
        elif self.path == "/flood":
            origin  = data.get("origin", NODE_ID)
            msg_id  = data.get("msg_id",  "")
            payload = data.get("payload", "")
            ttl     = int(data.get("ttl", 8))
            uid     = f"{origin}:{msg_id}"

            with flood_lock:
                if uid in flood_seen:
                    send_json(self, {"status": "seen"})
                    return
                flood_seen.add(uid)

            vc_increment()

            # HELLO processing — register sender as a known neighbor
            try:
                parsed = json.loads(payload)
                if parsed.get("type") == "HELLO":
                    sender = parsed.get("from")
                    if sender and sender != NODE_ID:
                        with neighbors_lock:
                            if sender not in neighbors:
                                neighbors[sender] = _default_metrics()
                                _log("hello_discovered", f"neighbor={sender}")
            except (json.JSONDecodeError, AttributeError):
                pass

            with store_lock:
                data_store[f"flood:{msg_id}"] = {
                    "value":  payload,
                    "vc":     vc_snapshot(),
                    "origin": origin,
                }
            recent_messages.append({"id": msg_id,
                "action": "flood_received", "ts": time.time()})

            if ttl <= 1:
                send_json(self, {"status": "ttl_expired"})
                return

            fwd = {**data, "ttl": ttl - 1}
            with neighbors_lock:
                fwd_nids = [nid for nid, m in neighbors.items()
                            if m.get("status") != "down"]
            for nid in fwd_nids:
                if ping_node(nid):
                    try:
                        post_json(_peer_url(nid, "/flood"), fwd)
                    except Exception:
                        pass

            send_json(self, {"status": "ok"})

        # /store/write
        elif self.path == "/store/write":
            key   = data.get("key",   "")
            value = data.get("value", "")
            vc_increment()
            with store_lock:
                data_store[key] = {"value": value,
                                    "vc":    vc_snapshot(),
                                    "origin": NODE_ID}
            send_json(self, {"status": "ok", "vc": vc_snapshot()})

        # /store/sync
        elif self.path == "/store/sync":
            peer_store = data.get("store", {})
            peer_vc    = data.get("vc",    {})
            updates    = 0
            with store_lock:
                for key, entry in peer_store.items():
                    local = data_store.get(key)
                    if local is None:
                        data_store[key] = entry; updates += 1
                    else:
                        if sum(entry.get("vc", {}).values()) > \
                           sum(local.get("vc",  {}).values()):
                            data_store[key] = entry; updates += 1
            vc_merge(peer_vc)
            send_json(self, {"status": "ok", "updates": updates,
                              "vc": vc_snapshot()})

        # /store/get
        elif self.path == "/store/get":
            with store_lock:
                snap = dict(data_store)
            send_json(self, {"store": snap, "vc": vc_snapshot()})

        else:
            self.send_error(404)

    def log_message(self, *_):
        pass


# ─────────────────────────────────────────────────────────────────────────
# Startup
# ─────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # Feature 1 — heartbeat
    threading.Thread(target=_heartbeat_loop, daemon=True, name="heartbeat").start()
    # Feature 5 — gossip
    threading.Thread(target=_gossip_loop,    daemon=True, name="gossip").start()
    # Feature 4 — HELLO (delayed so HTTP server is bound first)
    def _delayed_hello():
        time.sleep(3)
        _broadcast_hello()
    threading.Thread(target=_delayed_hello, daemon=True, name="hello").start()

    print(f"[node {NODE_ID}] listening :{NODE_PORT} "
          f"hb={HEARTBEAT_INTERVAL}s gossip={GOSSIP_INTERVAL}s "
          f"ack_timeout={ACK_TIMEOUT}s retries={MAX_RETRIES}",
          flush=True)
    HTTPServer(("", NODE_PORT), Handler).serve_forever()
