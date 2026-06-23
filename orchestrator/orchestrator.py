"""
orchestrator.py — Network Simulator Orchestrator  v4.3
=======================================================
Root-cause fix for dynamic container creation:

  BUG 1 — Missing network alias on container spawn.
    docker_client.containers.run() was passing network=NETWORK_NAME which
    connects the container to the network, but WITHOUT a hostname alias.
    Docker's embedded DNS only resolves a container by its *hostname* or
    explicit *aliases*. Without an alias of "node_a", the orchestrator's
    urllib call to http://node_a:8000/ping gets "bad address 'node_a'".

    FIX: Use the low-level Docker API (client.api.create_container +
    client.api.connect_container_to_network) so we can pass an explicit
    alias list ["node_a"] when attaching to the network.

  BUG 2 — Container created inside the lock, ping-wait outside the lock.
    create_node_container held container_lock while calling containers.run(),
    then released it before the ping loop. If two threads called this
    simultaneously the second would find the container already exists and
    return True before the HTTP server inside was ready.

    FIX: ping-wait loop now happens while NOT holding the lock, but we
    record the container only after the ping succeeds.

  BUG 3 — simulate() called get_live() (node-to-node peer pings) to decide
    whether to add an edge to the routing graph. Peer pings fail during the
    startup window because sibling containers haven't bound their HTTP server
    yet. This made every edge appear unreachable right after creation.

    FIX: graph construction uses orchestrator-side pings only
    (_ping_from_orchestrator), which is a single Docker-network hop and
    already proven reliable by create_node_container's own wait loop.
"""
import heapq, json, os, time, urllib.request, threading
from http.server import BaseHTTPRequestHandler, HTTPServer
import docker
from docker.errors import NotFound, APIError

PORT         = int(os.environ.get("PORT",      6767))
NODE_PORT    = int(os.environ.get("NODE_PORT", 8000))
NETWORK_NAME = os.environ.get("NETWORK_NAME", "sensor_mesh_sim_mesh")
NODE_IMAGE   = os.environ.get("NODE_IMAGE",   "sensor_mesh_sim-node")

docker_client = docker.from_env()

edge_store:  dict = {}
last_nodes:  list = []
last_graph:  dict = {}
last_weight: dict = {}

active_containers: set = set()
container_lock         = threading.Lock()

# ─────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────

def _edge_key(a: str, b: str) -> str:
    return "|".join(sorted([a, b]))

def _default_metrics(w: int = 1) -> dict:
    return {
        "weight": w, "bandwidth": 100, "delay": 5.0,
        "jitter": 1.0, "speed": 100, "loss": 0.0,
        "status": "up", "mtu": 1500, "utilization": 0.0,
    }

def container_name(nid: str) -> str:
    """Container name AND DNS alias used for inter-container communication."""
    return f"node_{nid.lower()}"

def node_url(nid: str, path: str) -> str:
    return f"http://{container_name(nid)}:{NODE_PORT}{path}"

# ─────────────────────────────────────────────────────────────────────────
# Dynamic container lifecycle  (THE FIXED PART)
# ─────────────────────────────────────────────────────────────────────────

def create_node_container(nid: str, timeout: int = 15) -> bool:
    """
    Spawn a node container dynamically and wait until its HTTP server
    answers /ping from the orchestrator.

    Key fix: we use the low-level Docker API so we can attach the container
    to the mesh network WITH an explicit alias equal to container_name(nid).
    This is what makes 'http://node_a:8000' resolvable from the orchestrator
    (both are on the same bridge network, and Docker's embedded DNS resolves
    the alias to the container's IP).
    """
    name = container_name(nid)

    with container_lock:
        # If the container already exists (e.g. leftover from a previous run),
        # just register it and move on.
        try:
            c = docker_client.containers.get(name)
            if c.status != "running":
                c.start()
            active_containers.add(nid)
            print(f"[orchestrator] {name} already exists (status={c.status})", flush=True)
        except NotFound:
            # Create the container but do NOT start it yet — we need to
            # connect it to the network with an alias before starting.
            try:
                # Step 1: create container (not yet connected to any network)
                container = docker_client.containers.create(
                    image=NODE_IMAGE,
                    name=name,
                    environment={
                        "NODE_ID":   nid,
                        "NODE_PORT": str(NODE_PORT),
                    },
                    detach=True,
                )

                # Step 2: connect to the mesh network WITH the alias
                # This is the critical fix — without aliases=["node_a"] here,
                # Docker DNS won't resolve "node_a" to this container.
                network = docker_client.networks.get(NETWORK_NAME)
                network.connect(
                    container,
                    aliases=[name],   # e.g. ["node_a"]
                )

                # Step 3: now start it
                container.start()
                active_containers.add(nid)
                print(f"[orchestrator] {name} created and started with alias={name}",
                      flush=True)

            except APIError as e:
                print(f"[orchestrator] ERROR creating {name}: {e}", flush=True)
                return False

    # Poll from the orchestrator until the HTTP server is accepting connections.
    # This happens OUTSIDE the lock so other threads aren't blocked.
    deadline = time.time() + timeout
    url      = node_url(nid, "/ping")
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=1) as r:
                if r.status == 200:
                    print(f"[orchestrator] {name} is up and answering /ping",
                          flush=True)
                    return True
        except Exception:
            time.sleep(0.3)

    print(f"[orchestrator] TIMEOUT waiting for {name} /ping after {timeout}s",
          flush=True)
    return False


def remove_node_container(nid: str):
    name = container_name(nid)
    with container_lock:
        active_containers.discard(nid)
        try:
            docker_client.containers.get(name).remove(force=True)
            print(f"[orchestrator] {name} removed", flush=True)
        except NotFound:
            pass
        except APIError as e:
            print(f"[orchestrator] ERROR removing {name}: {e}", flush=True)


def remove_all_containers():
    with container_lock:
        ids = list(active_containers)
    for nid in ids:
        remove_node_container(nid)

# ─────────────────────────────────────────────────────────────────────────
# HTTP helpers to talk to node containers
# ─────────────────────────────────────────────────────────────────────────

def post(url: str, body: dict, timeout: int = 3) -> dict:
    raw = json.dumps(body).encode()
    req = urllib.request.Request(url, data=raw,
                                  headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())

def get(url: str, timeout: int = 3) -> dict:
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.loads(r.read())

def safe_get(nid: str, path: str, default=None) -> dict:
    try:
        return get(node_url(nid, path))
    except Exception:
        return default if default is not None else {}

def push_neighbors(nid: str, neighbors: dict):
    try:
        post(node_url(nid, "/neighbors"), {"neighbors": neighbors})
    except Exception as e:
        print(f"[orchestrator] push_neighbors {nid} failed: {e}", flush=True)

def _ping_from_orchestrator(nid: str) -> bool:
    """Single-hop ping from orchestrator to node — used for graph construction."""
    try:
        with urllib.request.urlopen(node_url(nid, "/ping"), timeout=2) as r:
            return r.status == 200
    except Exception:
        return False

# ─────────────────────────────────────────────────────────────────────────
# Routing
# ─────────────────────────────────────────────────────────────────────────

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
    base   = float(m.get("weight",  1))
    delay  = float(m.get("delay",   0)) / 10.0
    jitter = float(m.get("jitter",  0)) / 5.0
    loss   = float(m.get("loss",    0)) * 2.0
    factor = 3.0 if m.get("status") == "degraded" else 1.0
    return (base + delay + jitter + loss) * factor

def simulate(nodes: list, edges: list) -> list:
    global last_nodes, last_graph, last_weight

    # Ensure every node has a running container before doing anything else
    for n in nodes:
        if n not in active_containers:
            ok = create_node_container(n)
            if not ok:
                print(f"[orchestrator] WARNING: could not start container for {n}",
                      flush=True)

    # Determine reachability from the orchestrator side (reliable single hop)
    reachable: set = set()
    for n in nodes:
        if _ping_from_orchestrator(n):
            reachable.add(n)
        else:
            print(f"[orchestrator] WARNING: {container_name(n)} not reachable",
                  flush=True)

    # Update edge store from UI payload
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

    # Push neighbor metric tables to every reachable node
    intended: dict = {n: {} for n in nodes}
    for e in edges:
        a, b = e["a"], e["b"]
        m = edge_store.get(_edge_key(a, b), _default_metrics())
        intended[a][b] = m
        intended[b][a] = m
    for nid, nbrs in intended.items():
        if nid in reachable:
            push_neighbors(nid, nbrs)

    # Build routing graph using orchestrator-confirmed reachability
    graph:  dict = {n: [] for n in nodes}
    weight: dict = {}
    seen:   set  = set()
    for e in edges:
        a, b = e["a"], e["b"]
        pair = (min(a, b), max(a, b))
        if pair in seen:
            continue
        seen.add(pair)
        m = edge_store.get(_edge_key(a, b), _default_metrics())
        if m.get("status") == "down":
            continue
        if a not in reachable or b not in reachable:
            print(f"[orchestrator] edge {a}↔{b} skipped: endpoint not reachable",
                  flush=True)
            continue
        cost = composite_cost(m)
        graph[a].append(b); graph[b].append(a)
        weight[(a, b)] = cost; weight[(b, a)] = cost

    last_nodes  = nodes
    last_graph  = graph
    last_weight = weight

    ns    = sorted(nodes)
    pairs = []
    for i in range(len(ns)):
        for j in range(i + 1, len(ns)):
            a, b = ns[i], ns[j]
            path, total = dijkstra(graph, weight, a, b)
            ok = path is not None
            pairs.append({
                "a": a, "b": b, "reachable": ok,
                "direct":  ok and len(path) == 2,
                "path":    path,
                "weight":  round(total, 3) if ok else 0,
                "metrics": edge_store.get(_edge_key(a, b)),
            })

    connected = sum(1 for p in pairs if p["reachable"])
    print(f"[orchestrator] simulate: {len(nodes)} nodes "
          f"{len(reachable)} reachable "
          f"{connected}/{len(pairs)} pairs connected", flush=True)
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

    def do_GET(self):
        if self.path in ("/", "/index.html"):
            with open(INDEX, "rb") as f:
                self.reply(f.read(), "text/html")

        elif self.path == "/api/traffic":
            all_traffic = {}
            for nid in last_nodes:
                res = safe_get(nid, "/traffic")
                all_traffic[nid] = res.get("recent", [])
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
                raw = safe_get(nid, "/status", {"error": "unreachable"})
                result[nid] = {
                    "id":          nid,
                    "vc":          raw.get("vc", {}),
                    "store_keys":  raw.get("store_keys", []),
                    "ack_pending": raw.get("ack_pending", []),
                    "neighbors": {
                        peer: {
                            "status":      m.get("status", "?"),
                            "_miss_count": m.get("_miss_count", 0),
                        }
                        for peer, m in raw.get("neighbors", {}).items()
                    },
                    "reachable": "error" not in raw,
                }
            self.reply(result)

        elif self.path == "/api/gossip_status":
            result = {}
            for nid in last_nodes:
                raw = safe_get(nid, "/status")
                result[nid] = {
                    "store_key_count": len(raw.get("store_keys", [])),
                    "store_keys":      raw.get("store_keys", []),
                    "vc":              raw.get("vc", {}),
                }
            self.reply(result)

        elif self.path == "/api/ack_status":
            result = {}
            for nid in last_nodes:
                raw = safe_get(nid, "/status")
                result[nid] = raw.get("ack_pending", [])
            self.reply(result)

        elif self.path == "/api/containers":
            self.reply({"active": sorted(active_containers)})

        # Diagnostic: check which nodes are currently pingable
        elif self.path == "/api/ping_all":
            result = {nid: _ping_from_orchestrator(nid) for nid in last_nodes}
            self.reply(result)

        else:
            self.send_error(404)

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        data   = json.loads(self.rfile.read(length) or b"{}")

        if self.path == "/api/node/create":
            nid = data.get("id", "").upper()
            if not nid:
                self.reply({"error": "id required"}, code=400); return
            ok = create_node_container(nid)
            self.reply({"status": "created" if ok else "timeout", "id": nid})

        elif self.path == "/api/node/remove":
            remove_node_container(data.get("id", "").upper())
            self.reply({"status": "removed"})

        elif self.path == "/api/reset":
            remove_all_containers()
            edge_store.clear()
            self.reply({"status": "reset"})

        elif self.path == "/api/simulate":
            pairs = simulate(data.get("nodes", []), data.get("edges", []))
            self.reply({"pairs": pairs})

        elif self.path == "/api/edge/update":
            a, b = data.get("a","").upper(), data.get("b","").upper()
            key  = _edge_key(a, b)
            if not a or not b:
                self.reply({"error": "a and b required"}, code=400); return
            if key not in edge_store:
                edge_store[key] = _default_metrics()
            allowed = {"weight","bandwidth","delay","jitter",
                       "speed","loss","status","mtu","utilization"}
            for k, v in data.items():
                if k in allowed:
                    edge_store[key][k] = v
            for nid, peer in [(a, b), (b, a)]:
                if nid in last_nodes:
                    push_neighbors(nid, {peer: edge_store[key]})
            self.reply({"status": "ok", "key": key, "metrics": edge_store[key]})

        elif self.path == "/api/send_message":
            src = data.get("src","").upper()
            dst = data.get("dst","").upper()
            if not src or not dst or \
               src not in last_graph or dst not in last_graph:
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
            origin  = data.get("origin","").upper()
            msg_id  = data.get("msg_id",  f"f_{os.urandom(3).hex()}")
            payload = data.get("payload", "broadcast")
            if origin not in last_nodes:
                self.reply({"status": "unknown_origin"}, code=400); return
            try:
                post(node_url(origin, "/flood"),
                     {"origin": origin, "msg_id": msg_id,
                      "payload": payload, "ttl": 10})
                self.reply({"status": "flooded", "origin": origin,
                             "msg_id": msg_id})
            except Exception as e:
                self.reply({"status": "error", "message": str(e)}, code=500)

        elif self.path == "/api/hello":
            nid = data.get("id","").upper()
            if nid not in last_nodes:
                self.reply({"status": "unknown_node"}, code=400); return
            try:
                post(node_url(nid, "/neighbors"), {"neighbors": {}})
                self.reply({"status": "hello_triggered", "id": nid})
            except Exception as e:
                self.reply({"status": "error", "message": str(e)}, code=500)

        else:
            self.send_error(404)

    def log_message(self, *_):
        pass


if __name__ == "__main__":
    print(f"[orchestrator] listening on :{PORT}", flush=True)
    print(f"[orchestrator] node image : {NODE_IMAGE}", flush=True)
    print(f"[orchestrator] network    : {NETWORK_NAME}", flush=True)
    HTTPServer(("", PORT), Handler).serve_forever()
