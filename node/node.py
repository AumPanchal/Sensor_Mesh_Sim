"""
node.py  — Network Simulator Node Agent
Each node runs as an isolated HTTP server inside a Docker container.
It stores per-neighbor link metrics (weight, bandwidth, delay, jitter,
speed, loss, status) and exposes them for the orchestrator and UI.
"""
import json, os, time, random, urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer
from collections import deque

NODE_ID   = os.environ["NODE_ID"]
NODE_PORT = int(os.environ.get("NODE_PORT", 8000))

# ── Link-metric store ─────────────────────────────────────────────────────
# neighbors[nid] = {
#   "weight": int,
#   "bandwidth": int,   # Mbps
#   "delay":     float, # ms
#   "jitter":    float, # ms
#   "speed":     int,   # Mbps (physical link speed)
#   "loss":      float, # % packet loss  0-100
#   "status":    str,   # "up" | "degraded" | "down"
#   "mtu":       int,   # bytes
# }
neighbors: dict[str, dict] = {}

# ── Vector clock ──────────────────────────────────────────────────────────
vector_clock: dict[str, int] = {NODE_ID: 0}

# ── Ephemeral event buffer (last 20, no memory leak) ─────────────────────
recent_messages: deque = deque(maxlen=20)

# ── Flood seen-set (origin:msgId) ────────────────────────────────────────
flood_seen: set = set()

# ── Application-layer data store  key → {value, vc, origin} ─────────────
data_store: dict[str, dict] = {}

# ─────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────

def _default_metrics() -> dict:
    return {
        "weight":    1,
        "bandwidth": 100,
        "delay":     5.0,
        "jitter":    1.0,
        "speed":     100,
        "loss":      0.0,
        "status":    "up",
        "mtu":       1500,
    }

def send_json(handler, body, code=200):
    raw = json.dumps(body).encode()
    handler.send_response(code)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Content-Length", str(len(raw)))
    handler.end_headers()
    handler.wfile.write(raw)

def vc_increment():
    vector_clock[NODE_ID] = vector_clock.get(NODE_ID, 0) + 1

def vc_merge(incoming: dict):
    for k, v in incoming.items():
        vector_clock[k] = max(vector_clock.get(k, 0), v)

def ping(nid: str) -> bool:
    """Return True if the peer node answers /ping within 1 s."""
    try:
        url = f"http://node_{nid.lower()}:{NODE_PORT}/ping"
        with urllib.request.urlopen(url, timeout=1) as r:
            return r.status == 200
    except Exception:
        return False

def post_json(url: str, body: dict) -> dict:
    raw = json.dumps(body).encode()
    req = urllib.request.Request(
        url, data=raw,
        headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=2) as r:
        return json.loads(r.read())

# ─────────────────────────────────────────────────────────────────────────
# HTTP Handler
# ─────────────────────────────────────────────────────────────────────────

class Handler(BaseHTTPRequestHandler):

    # ── GET ──────────────────────────────────────────────────────────────

    def do_GET(self):
        if self.path == "/ping":
            send_json(self, {"id": NODE_ID, "ok": True})

        elif self.path == "/status":
            # Full node state for the UI
            send_json(self, {
                "id":         NODE_ID,
                "vc":         vector_clock,
                "store_keys": list(data_store.keys()),
                "neighbors":  neighbors,
            })

        elif self.path == "/metrics":
            # Per-link metrics table (used by the orchestrator for the report)
            rows = []
            for nid, m in neighbors.items():
                rows.append({"from": NODE_ID, "to": nid, **m})
            send_json(self, {"id": NODE_ID, "links": rows})

        elif self.path == "/traffic":
            send_json(self, {"id": NODE_ID, "recent": list(recent_messages)})

        else:
            self.send_error(404)

    # ── POST ─────────────────────────────────────────────────────────────

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        data   = json.loads(self.rfile.read(length) or b"{}")

        # ── /neighbors  ─  set neighbor list + metrics ───────────────────
        if self.path == "/neighbors":
            for nid, raw_metrics in data.get("neighbors", {}).items():
                base = _default_metrics()
                base.update({k: raw_metrics[k] for k in raw_metrics if k in base})
                # Always cast types correctly
                base["weight"]    = int(base["weight"])
                base["bandwidth"] = int(base["bandwidth"])
                base["delay"]     = float(base["delay"])
                base["jitter"]    = float(base["jitter"])
                base["speed"]     = int(base["speed"])
                base["loss"]      = float(base["loss"])
                base["mtu"]       = int(base["mtu"])
                neighbors[nid] = base
            send_json(self, {"set": list(neighbors.keys())})

        # ── /reach  ─  probe which neighbors are currently reachable ─────
        elif self.path == "/reach":
            live = [nid for nid in neighbors if ping(nid)]
            send_json(self, {
                "id":      NODE_ID,
                "live":    live,
                "metrics": neighbors,
            })

        # ── /message  ─  receive / forward a routed envelope ─────────────
        elif self.path == "/message":
            msg_id = data.get("msg_id", "unknown")
            path   = data.get("path", [])
            vc_increment()

            if path and path[0] == NODE_ID:
                recent_messages.append({"id": msg_id, "action": "generated",
                                         "ts": time.time()})
            else:
                recent_messages.append({"id": msg_id, "action": "received",
                                         "ts": time.time()})

            if path and path[-1] != NODE_ID:
                try:
                    nxt = path[path.index(NODE_ID) + 1]
                    url = f"http://node_{nxt.lower()}:{NODE_PORT}/message"
                    post_json(url, data)
                    recent_messages.append({"id": msg_id,
                                             "action": f"forwarded_to_{nxt}",
                                             "ts": time.time()})
                except Exception:
                    recent_messages.append({"id": msg_id, "action": "dropped",
                                             "ts": time.time()})

            send_json(self, {"status": "ok"})

        # ── /flood  ─  application-layer broadcast ────────────────────────
        elif self.path == "/flood":
            origin = data.get("origin", NODE_ID)
            msg_id = data.get("msg_id", "")
            payload= data.get("payload", "")
            uid    = f"{origin}:{msg_id}"

            if uid in flood_seen:
                send_json(self, {"status": "seen"})
                return

            flood_seen.add(uid)
            vc_increment()
            data_store[f"flood:{msg_id}"] = {
                "value":  payload,
                "vc":     dict(vector_clock),
                "origin": origin,
            }
            recent_messages.append({"id": msg_id, "action": "flood_received",
                                     "ts": time.time()})

            # Forward to all live UP neighbors
            for nid, m in neighbors.items():
                if m.get("status", "up") == "down":
                    continue
                if ping(nid):
                    try:
                        url = f"http://node_{nid.lower()}:{NODE_PORT}/flood"
                        post_json(url, data)
                    except Exception:
                        pass

            send_json(self, {"status": "ok"})

        # ── /store/write  ─  anti-entropy write ───────────────────────────
        elif self.path == "/store/write":
            key   = data.get("key", "")
            value = data.get("value", "")
            vc_increment()
            data_store[key] = {"value": value, "vc": dict(vector_clock),
                                "origin": NODE_ID}
            send_json(self, {"status": "ok", "vc": vector_clock})

        # ── /store/sync  ─  anti-entropy push from peer ───────────────────
        elif self.path == "/store/sync":
            peer_store = data.get("store", {})
            peer_vc    = data.get("vc", {})
            updates    = 0
            for key, entry in peer_store.items():
                local = data_store.get(key)
                if local is None:
                    data_store[key] = entry
                    updates += 1
                else:
                    # Compare VC sums as LWW tiebreaker
                    sum_peer  = sum(entry.get("vc", {}).values())
                    sum_local = sum(local.get("vc", {}).values())
                    if sum_peer > sum_local:
                        data_store[key] = entry
                        updates += 1
            vc_merge(peer_vc)
            send_json(self, {"status": "ok", "updates": updates,
                              "vc": vector_clock})

        # ── /store/get  ─  read the full store (for sync) ────────────────
        elif self.path == "/store/get":
            send_json(self, {"store": data_store, "vc": vector_clock})

        else:
            self.send_error(404)

    def log_message(self, *_):
        pass


if __name__ == "__main__":
    print(f"[node {NODE_ID}] listening on :{NODE_PORT}", flush=True)
    HTTPServer(("", NODE_PORT), Handler).serve_forever()
