"""
orchestrator.py  — Network Simulator Orchestrator
Serves the UI, runs Dijkstra, manages full link-metric state,
and bridges the browser ↔ node containers.
"""
import heapq, json, os, time, urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer

PORT      = int(os.environ.get("PORT",      6767))
NODE_PORT = int(os.environ.get("NODE_PORT", 8000))

# ── In-memory edge-metric store ───────────────────────────────────────────
# key: frozenset({a, b})  → metrics dict
edge_store: dict = {}          # "A|B" → {**metrics}
last_nodes: list = []
last_graph: dict = {}
last_weight: dict= {}

def _edge_key(a: str, b: str) -> str:
    return "|".join(sorted([a, b]))

def _default_metrics(w: int = 1) -> dict:
    return {
        "weight":    w,
        "bandwidth": 100,    # Mbps
        "delay":     5.0,    # ms
        "jitter":    1.0,    # ms
        "speed":     100,    # Mbps
        "loss":      0.0,    # %
        "status":    "up",
        "mtu":       1500,
        "utilization": 0.0,  # % (for reporting)
    }

# ─────────────────────────────────────────────────────────────────────────
# HTTP helpers
# ─────────────────────────────────────────────────────────────────────────

def node_url(nid: str, path: str) -> str:
    return f"http://node_{nid.lower()}:{NODE_PORT}{path}"

def post(url: str, body: dict) -> dict:
    raw = json.dumps(body).encode()
    req = urllib.request.Request(url, data=raw,
                                  headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=2) as r:
        return json.loads(r.read())

def get(url: str) -> dict:
    with urllib.request.urlopen(url, timeout=2) as r:
        return json.loads(r.read())

# ─────────────────────────────────────────────────────────────────────────
# Core logic
# ─────────────────────────────────────────────────────────────────────────

def push_neighbors(nid: str, neighbors: dict):
    """Send full metric dicts to a node."""
    try:
        post(node_url(nid, "/neighbors"), {"neighbors": neighbors})
    except Exception:
        pass

def get_live(nid: str) -> dict:
    try:
        return post(node_url(nid, "/reach"), {})
    except Exception:
        return {"live": [], "metrics": {}}

def dijkstra(graph: dict, weight: dict, src: str, dst: str):
    if src == dst:
        return [src], 0
    dist, prev, pq, vis = {src: 0}, {}, [(0, src)], set()
    while pq:
        d, u = heapq.heappop(pq)
        if u in vis:
            continue
        vis.add(u)
        if u == dst:
            break
        for v in graph.get(u, []):
            nd = d + weight.get((u, v), 1)
            if v not in dist or nd < dist[v]:
                dist[v] = nd
                prev[v] = u
                heapq.heappush(pq, (nd, v))
    if dst not in dist:
        return None, None
    path = [dst]
    while path[-1] != src:
        path.append(prev[path[-1]])
    path.reverse()
    return path, dist[dst]

def composite_cost(m: dict) -> float:
    """Convert metrics to a single routing cost (lower = better)."""
    if m.get("status") == "down":
        return float("inf")
    base   = float(m.get("weight", 1))
    delay  = float(m.get("delay",  0)) / 10.0
    jitter = float(m.get("jitter", 0)) / 5.0
    loss   = float(m.get("loss",   0)) * 2.0
    factor = 3.0 if m.get("status") == "degraded" else 1.0
    return (base + delay + jitter + loss) * factor

def simulate(nodes: list, edges: list) -> list:
    global last_nodes, last_graph, last_weight

    # Merge UI-supplied edges with our stored metrics
    for e in edges:
        key = _edge_key(e["a"], e["b"])
        if key not in edge_store:
            edge_store[key] = _default_metrics(int(e.get("w", 1)))
        else:
            # Update weight if the user changed it in the edge weight input
            edge_store[key]["weight"] = int(e.get("w", edge_store[key]["weight"]))
        # Propagate status if edge is toggled down in the UI
        if not e.get("up", True):
            edge_store[key]["status"] = "down"
        elif edge_store[key]["status"] == "down" and e.get("up", True):
            edge_store[key]["status"] = "up"

    # Build intended neighbor maps with full metrics and push to each node
    intended: dict[str, dict] = {n: {} for n in nodes}
    for e in edges:
        a, b = e["a"], e["b"]
        key  = _edge_key(a, b)
        m    = edge_store.get(key, _default_metrics())
        intended[a][b] = m
        intended[b][a] = m

    for nid, nbrs in intended.items():
        push_neighbors(nid, nbrs)

    # Probe liveness
    live: dict[str, set] = {}
    for nid in nodes:
        info = get_live(nid)
        live[nid] = set(info.get("live", []))

    # Build routing graph (only live + non-down links)
    graph:  dict[str, list]  = {n: [] for n in nodes}
    weight: dict[tuple, float] = {}
    seen:   set = set()

    for e in edges:
        a, b = e["a"], e["b"]
        pair = (min(a, b), max(a, b))
        if pair in seen:
            continue
        seen.add(pair)
        key = _edge_key(a, b)
        m   = edge_store.get(key, _default_metrics())
        if m.get("status") == "down":
            continue
        if b in live.get(a, set()) and a in live.get(b, set()):
            cost = composite_cost(m)
            graph[a].append(b); graph[b].append(a)
            weight[(a, b)] = cost
            weight[(b, a)] = cost

    last_nodes  = nodes
    last_graph  = graph
    last_weight = weight

    ns = sorted(nodes)
    pairs = []
    for i in range(len(ns)):
        for j in range(i + 1, len(ns)):
            a, b = ns[i], ns[j]
            path, total = dijkstra(graph, weight, a, b)
            reachable = path is not None
            pairs.append({
                "a": a, "b": b,
                "reachable": reachable,
                "direct":    reachable and len(path) == 2,
                "path":      path,
                "weight":    round(total, 3) if reachable else 0,
                "metrics":   edge_store.get(_edge_key(a, b)),
            })
    return pairs

# ─────────────────────────────────────────────────────────────────────────
# HTTP Handler
# ─────────────────────────────────────────────────────────────────────────
INDEX = os.path.join(os.path.dirname(__file__), "index.html")

class Handler(BaseHTTPRequestHandler):

    def reply(self, body, ctype="application/json", code=200):
        if isinstance(body, (dict, list)):
            body = json.dumps(body).encode()
        elif isinstance(body, str):
            body = body.encode()
        self.send_response(code)
        self.send_header("Content-Type",   ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin",  "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    # ── GET ──────────────────────────────────────────────────────────────

    def do_GET(self):
        if self.path in ("/", "/index.html"):
            with open(INDEX, "rb") as f:
                self.reply(f.read(), "text/html")

        elif self.path == "/api/traffic":
            all_traffic = {}
            for nid in last_nodes:
                try:
                    res = get(node_url(nid, "/traffic"))
                    all_traffic[nid] = res.get("recent", [])
                except Exception:
                    all_traffic[nid] = []
            self.reply(all_traffic)

        elif self.path == "/api/metrics":
            # Return the full edge-metric store as a list of rows
            rows = []
            for key, m in edge_store.items():
                a, b = key.split("|")
                rows.append({"link": f"{a}↔{b}", "a": a, "b": b, **m})
            self.reply({"edges": rows})

        elif self.path == "/api/node_status":
            # Aggregate /status from every known node
            result = {}
            for nid in last_nodes:
                try:
                    result[nid] = get(node_url(nid, "/status"))
                except Exception:
                    result[nid] = {"error": "unreachable"}
            self.reply(result)

        else:
            self.send_error(404)

    # ── POST ─────────────────────────────────────────────────────────────

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        data   = json.loads(self.rfile.read(length) or b"{}")

        # ── /api/simulate ────────────────────────────────────────────────
        if self.path == "/api/simulate":
            pairs = simulate(data.get("nodes", []), data.get("edges", []))
            self.reply({"pairs": pairs})

        # ── /api/edge/update  ─  UI sends updated metrics for one edge ───
        elif self.path == "/api/edge/update":
            a   = data.get("a", "").upper()
            b   = data.get("b", "").upper()
            key = _edge_key(a, b)
            if not a or not b:
                self.reply({"error": "a and b required"}, code=400)
                return

            if key not in edge_store:
                edge_store[key] = _default_metrics()

            allowed = {"weight","bandwidth","delay","jitter","speed",
                       "loss","status","mtu","utilization"}
            for k, v in data.items():
                if k in allowed:
                    edge_store[key][k] = v

            # Re-push updated metrics to both nodes immediately
            for nid, peer in [(a, b), (b, a)]:
                if nid in last_nodes:
                    push_neighbors(nid, {peer: edge_store[key]})

            self.reply({"status": "ok", "key": key, "metrics": edge_store[key]})

        # ── /api/send_message ────────────────────────────────────────────
        elif self.path == "/api/send_message":
            src = data.get("src", "").upper()
            dst = data.get("dst", "").upper()

            if not src or not dst or \
               src not in last_graph or dst not in last_graph:
                self.reply({"status": "invalid_nodes"}, code=400)
                return

            path, total = dijkstra(last_graph, last_weight, src, dst)
            if path:
                envelope = {
                    "msg_id": f"sim_{os.urandom(4).hex()}",
                    "path":   path,
                }
                try:
                    post(node_url(src, "/message"), envelope)
                    self.reply({
                        "status": "dispatched",
                        "path":   path,
                        "cost":   round(total, 3),
                    })
                except Exception as e:
                    self.reply({"status": "error", "message": str(e)}, code=500)
            else:
                self.reply({"status": "unreachable"})

        # ── /api/flood ───────────────────────────────────────────────────
        elif self.path == "/api/flood":
            origin  = data.get("origin", "").upper()
            msg_id  = data.get("msg_id",  f"f_{os.urandom(3).hex()}")
            payload = data.get("payload", "broadcast")

            if origin not in last_nodes:
                self.reply({"status": "unknown_origin"}, code=400)
                return
            try:
                post(node_url(origin, "/flood"), {
                    "origin":  origin,
                    "msg_id":  msg_id,
                    "payload": payload,
                })
                self.reply({"status": "flooded", "origin": origin,
                             "msg_id": msg_id})
            except Exception as e:
                self.reply({"status": "error", "message": str(e)}, code=500)

        else:
            self.send_error(404)

    def log_message(self, *_):
        pass


if __name__ == "__main__":
    print(f"[orchestrator] listening on :{PORT}", flush=True)
    HTTPServer(("", PORT), Handler).serve_forever()
