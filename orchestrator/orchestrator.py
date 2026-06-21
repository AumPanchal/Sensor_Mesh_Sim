"""
orchestrator.py — Network Simulator Orchestrator
Serves the UI, runs Dijkstra, manages full link-metric state,
dynamically spins up/tears down node containers, and bridges
the browser <-> node containers.
"""
import heapq, json, os, time, urllib.request, threading
from http.server import BaseHTTPRequestHandler, HTTPServer
import docker
from docker.errors import NotFound, APIError

PORT         = int(os.environ.get("PORT", 6767))
NODE_PORT    = int(os.environ.get("NODE_PORT", 8000))
NETWORK_NAME = os.environ.get("NETWORK_NAME", "sensor_mesh_sim_mesh")
NODE_IMAGE   = os.environ.get("NODE_IMAGE", "sensor_mesh_sim-node")

docker_client = docker.from_env()

# ── In-memory edge-metric store ───────────────────────────────────────────
edge_store: dict = {}
last_nodes: list = []
last_graph: dict = {}
last_weight: dict = {}

# Track which node IDs currently have live containers
active_containers: set = set()
container_lock = threading.Lock()

def _edge_key(a: str, b: str) -> str:
    return "|".join(sorted([a, b]))

def _default_metrics(w: int = 1) -> dict:
    return {
        "weight": w, "bandwidth": 100, "delay": 5.0, "jitter": 1.0,
        "speed": 100, "loss": 0.0, "status": "up", "mtu": 1500,
        "utilization": 0.0,
    }

# ─────────────────────────────────────────────────────────────────────────
# Container lifecycle
# ─────────────────────────────────────────────────────────────────────────

def container_name(node_id: str) -> str:
    return f"node_{node_id.lower()}"

def create_node_container(node_id: str, timeout: int = 10) -> bool:
    """Create and start a container for node_id. Returns True once it answers /ping."""
    name = container_name(node_id)
    with container_lock:
        try:
            docker_client.containers.get(name)
            active_containers.add(node_id)
            return True
        except NotFound:
            pass
        try:
            docker_client.containers.run(
                image=NODE_IMAGE,
                name=name,
                environment={"NODE_ID": node_id, "NODE_PORT": str(NODE_PORT)},
                network=NETWORK_NAME,
                detach=True,
            )
            active_containers.add(node_id)
        except APIError as e:
            print(f"[orchestrator] failed to create {name}: {e}", flush=True)
            return False

    # Wait for it to actually come up before returning
    deadline = time.time() + timeout
    url = node_url(node_id, "/ping")
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=1) as r:
                if r.status == 200:
                    return True
        except Exception:
            time.sleep(0.3)
    return False

def remove_node_container(node_id: str):
    name = container_name(node_id)
    with container_lock:
        active_containers.discard(node_id)
        try:
            c = docker_client.containers.get(name)
            c.remove(force=True)
        except NotFound:
            pass
        except APIError as e:
            print(f"[orchestrator] failed to remove {name}: {e}", flush=True)

def remove_all_containers():
    with container_lock:
        ids = list(active_containers)
    for nid in ids:
        remove_node_container(nid)

# ─────────────────────────────────────────────────────────────────────────
# HTTP helpers to talk to node containers
# ─────────────────────────────────────────────────────────────────────────

def node_url(nid: str, path: str) -> str:
    return f"http://{container_name(nid)}:{NODE_PORT}{path}"

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
# Core simulation logic
# ─────────────────────────────────────────────────────────────────────────

def push_neighbors(nid: str, neighbors: dict):
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
    if m.get("status") == "down":
        return float("inf")
    base   = float(m.get("weight", 1))
    delay  = float(m.get("delay", 0)) / 10.0
    jitter = float(m.get("jitter", 0)) / 5.0
    loss   = float(m.get("loss", 0)) * 2.0
    factor = 3.0 if m.get("status") == "degraded" else 1.0
    return (base + delay + jitter + loss) * factor

def simulate(nodes: list, edges: list) -> list:
    global last_nodes, last_graph, last_weight

    # Ensure a container exists for every node before doing anything else
    for n in nodes:
        if n not in active_containers:
            create_node_container(n)

    for e in edges:
        key = _edge_key(e["a"], e["b"])
        if key not in edge_store:
            edge_store[key] = _default_metrics(int(e.get("w", 1)))
        else:
            edge_store[key]["weight"] = int(e.get("w", edge_store[key]["weight"]))
        if not e.get("up", True):
            edge_store[key]["status"] = "down"
        elif edge_store[key]["status"] == "down" and e.get("up", True):
            edge_store[key]["status"] = "up"

    intended: dict = {n: {} for n in nodes}
    for e in edges:
        a, b = e["a"], e["b"]
        m = edge_store.get(_edge_key(a, b), _default_metrics())
        intended[a][b] = m
        intended[b][a] = m

    for nid, nbrs in intended.items():
        push_neighbors(nid, nbrs)

    live: dict = {}
    for nid in nodes:
        info = get_live(nid)
        live[nid] = set(info.get("live", []))

    graph:  dict = {n: [] for n in nodes}
    weight: dict = {}
    seen: set = set()

    for e in edges:
        a, b = e["a"], e["b"]
        pair = (min(a, b), max(a, b))
        if pair in seen:
            continue
        seen.add(pair)
        m = edge_store.get(_edge_key(a, b), _default_metrics())
        if m.get("status") == "down":
            continue
        if b in live.get(a, set()) and a in live.get(b, set()):
            cost = composite_cost(m)
            graph[a].append(b); graph[b].append(a)
            weight[(a, b)] = cost
            weight[(b, a)] = cost

    last_nodes, last_graph, last_weight = nodes, graph, weight

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
                "direct": reachable and len(path) == 2,
                "path": path,
                "weight": round(total, 3) if reachable else 0,
                "metrics": edge_store.get(_edge_key(a, b)),
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
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
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
            rows = []
            for key, m in edge_store.items():
                a, b = key.split("|")
                rows.append({"link": f"{a}↔{b}", "a": a, "b": b, **m})
            self.reply({"edges": rows})

        elif self.path == "/api/node_status":
            result = {}
            for nid in last_nodes:
                try:
                    result[nid] = get(node_url(nid, "/status"))
                except Exception:
                    result[nid] = {"error": "unreachable"}
            self.reply(result)

        elif self.path == "/api/containers":
            self.reply({"active": sorted(active_containers)})

        else:
            self.send_error(404)

    # ── POST ─────────────────────────────────────────────────────────────
    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        data = json.loads(self.rfile.read(length) or b"{}")

        if self.path == "/api/node/create":
            node_id = data.get("id", "").upper()
            if not node_id:
                self.reply({"error": "id required"}, code=400); return
            ok = create_node_container(node_id)
            self.reply({"status": "created" if ok else "timeout", "id": node_id})

        elif self.path == "/api/node/remove":
            node_id = data.get("id", "").upper()
            remove_node_container(node_id)
            self.reply({"status": "removed", "id": node_id})

        elif self.path == "/api/reset":
            remove_all_containers()
            edge_store.clear()
            self.reply({"status": "reset"})

        elif self.path == "/api/simulate":
            pairs = simulate(data.get("nodes", []), data.get("edges", []))
            self.reply({"pairs": pairs})

        elif self.path == "/api/edge/update":
            a = data.get("a", "").upper()
            b = data.get("b", "").upper()
            key = _edge_key(a, b)
            if not a or not b:
                self.reply({"error": "a and b required"}, code=400); return
            if key not in edge_store:
                edge_store[key] = _default_metrics()
            allowed = {"weight","bandwidth","delay","jitter","speed",
                       "loss","status","mtu","utilization"}
            for k, v in data.items():
                if k in allowed:
                    edge_store[key][k] = v
            for nid, peer in [(a, b), (b, a)]:
                if nid in last_nodes:
                    push_neighbors(nid, {peer: edge_store[key]})
            self.reply({"status": "ok", "key": key, "metrics": edge_store[key]})

        elif self.path == "/api/send_message":
            src = data.get("src", "").upper()
            dst = data.get("dst", "").upper()
            if not src or not dst or src not in last_graph or dst not in last_graph:
                self.reply({"status": "invalid_nodes"}, code=400); return
            path, total = dijkstra(last_graph, last_weight, src, dst)
            if path:
                envelope = {"msg_id": f"sim_{os.urandom(4).hex()}", "path": path}
                try:
                    post(node_url(src, "/message"), envelope)
                    self.reply({"status": "dispatched", "path": path,
                                "cost": round(total, 3)})
                except Exception as e:
                    self.reply({"status": "error", "message": str(e)}, code=500)
            else:
                self.reply({"status": "unreachable"})

        elif self.path == "/api/flood":
            origin = data.get("origin", "").upper()
            msg_id = data.get("msg_id", f"f_{os.urandom(3).hex()}")
            payload = data.get("payload", "broadcast")
            if origin not in last_nodes:
                self.reply({"status": "unknown_origin"}, code=400); return
            try:
                post(node_url(origin, "/flood"),
                     {"origin": origin, "msg_id": msg_id, "payload": payload})
                self.reply({"status": "flooded", "origin": origin, "msg_id": msg_id})
            except Exception as e:
                self.reply({"status": "error", "message": str(e)}, code=500)

        else:
            self.send_error(404)

    def log_message(self, *_):
        pass


if __name__ == "__main__":
    print(f"[orchestrator] listening on :{PORT}", flush=True)
    HTTPServer(("", PORT), Handler).serve_forever()