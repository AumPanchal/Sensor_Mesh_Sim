import heapq, json, os, urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer

PORT = int(os.environ.get("PORT", 6767))
NODE_PORT = int(os.environ.get("NODE_PORT", 8000))

def node_url(nid, path):
    return f"http://node_{nid.lower()}:{NODE_PORT}{path}"

def post(url, body):
    raw = json.dumps(body).encode()
    req = urllib.request.Request(url, data=raw, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=2) as r:
        return json.loads(r.read())

def push_neighbors(nid, neighbors):
    #tell node who its current neighbors are
    try:
        post(node_url(nid, "/neighbors"), {"weights": neighbors})
    except Exception as e:
        pass

def get_live(nid):
    #ask node which neighbors it can actually ping
    try:
        return post(node_url(nid, "/reach"), {})
    except Exception as e:
        return {"live": [], "weights": {}}

def dijkstra(graph, weight, src, dst):
    #find shortest weighted path from src to dst
    if src == dst:
        return [src], 0
    dist = {src: 0}
    prev = {}
    pq = [(0, src)]
    vis = set()
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

def simulate(nodes, edges):
    #build intended neighbor map from up edges
    intended = {n: {} for n in nodes}
    for e in edges:
        a, b, w = e["a"], e["b"], int(e.get("w", 1))
        intended[a][b] = w
        intended[b][a] = w

    #push neighbor lists to all nodes
    for nid, nbrs in intended.items():
        push_neighbors(nid, nbrs)

    #ask each node which neighbors it can actually ping
    live = {}
    for nid in nodes:
        info = get_live(nid)
        live[nid] = set(info.get("live", []))

    #build confirmed graph — link only counts if both sides can ping each other
    graph = {n: [] for n in nodes}
    weight = {}
    seen = set()
    for e in edges:
        a, b, w = e["a"], e["b"], int(e.get("w", 1))
        pair = (min(a, b), max(a, b))
        if pair in seen:
            continue
        seen.add(pair)
        if b in live.get(a, set()) and a in live.get(b, set()):
            graph[a].append(b)
            graph[b].append(a)
            weight[(a, b)] = w
            weight[(b, a)] = w

    #run dijkstra on every node pair and return results
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
                "weight": total if reachable else 0,
            })
    return pairs

INDEX = os.path.join(os.path.dirname(__file__), "index.html")

class Handler(BaseHTTPRequestHandler):
    def send(self, body, ctype="application/json", code=200):
        if isinstance(body, (dict, list)):
            body = json.dumps(body).encode()
        elif isinstance(body, str):
            body = body.encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path in ("/", "/index.html"):
            with open(INDEX, "rb") as f:
                self.send(f.read(), "text/html")
        else:
            self.send_error(404)

    def do_POST(self):
        if self.path == "/api/simulate":
            length = int(self.headers.get("Content-Length", 0))
            data = json.loads(self.rfile.read(length) or b"{}")
            pairs = simulate(data.get("nodes", []), data.get("edges", []))
            self.send({"pairs": pairs})
        else:
            self.send_error(404)

    def log_message(self, *a):
        pass

if __name__ == "__main__":
    HTTPServer(("", PORT), Handler).serve_forever()