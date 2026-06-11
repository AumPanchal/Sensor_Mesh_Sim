#http://localhost:6767

import json
import os
import heapq
from http.server import BaseHTTPRequestHandler, HTTPServer
from mesh import Mesh

PORT = 6767


def dijkstra(m, weight, src, dst):
    #lowest-total-weight path from src to dst.
    #returns (path_list, total_weight) or (None, None) if unreachable.
    if src == dst:
        return [src], 0
    dist = {src: 0}
    prev = {}
    pq = [(0, src)]
    visited = set()
    while pq:
        d, u = heapq.heappop(pq)
        if u in visited:
            continue
        visited.add(u)
        if u == dst:
            break
        for v in m.graph.get(u, []):
            nd = d + weight[(u, v)]
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


def connectivity(nodes, edges):
    #build fresh mesh from what the page sent (edges here are the UP ones)
    m = Mesh()
    for n in nodes:
        m.add_node(n)

    weight = {}
    for e in edges:
        m.connect(e["a"], e["b"])
        w = e.get("w", 1)
        weight[(e["a"], e["b"])] = w
        weight[(e["b"], e["a"])] = w

    ns = m.nodes()
    pairs = []
    for i in range(len(ns)):
        for j in range(i + 1, len(ns)):
            a, b = ns[i], ns[j]
            path, total = dijkstra(m, weight, a, b)
            reachable = path is not None
            pairs.append({
                "a": a, "b": b,
                "reachable": reachable,
                "direct": reachable and len(path) == 2,
                "path": path,
                "weight": total if reachable else 0,
            })
    return pairs


class Handler(BaseHTTPRequestHandler):
    def _send(self, body, ctype="application/json", code=200):
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
            with open(os.path.join(os.path.dirname(__file__), "index.html"), "rb") as f:
                self._send(f.read(), "text/html")
        else:
            self.send_error(404)

    def do_POST(self):
        if self.path == "/api/simulate":
            n = int(self.headers.get("Content-Length", 0))
            data = json.loads(self.rfile.read(n) or "{}")
            pairs = connectivity(data.get("nodes", []), data.get("edges", []))
            self._send({"pairs": pairs})
        else:
            self.send_error(404)

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    HTTPServer(("", PORT), Handler).serve_forever()