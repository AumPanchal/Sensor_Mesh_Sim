#http://localhost:6767

import json
import os
from collections import deque
from http.server import BaseHTTPRequestHandler, HTTPServer
from mesh import Mesh

PORT = 6767

def connectivity(nodes, edges):
    #build a fresh mesh from what the page sent (edges here are the UP ones)
    m = Mesh()
    for n in nodes:
        m.add_node(n)
    for e in edges:
        m.connect(e["a"], e["b"])

    def path(a, b):
        if a == b:
            return [a]
        q = deque([[a]])
        seen = {a}
        while q:
            p = q.popleft()
            for nb in m.graph.get(p[-1], []):
                if nb == b:
                    return p + [nb]
                if nb not in seen:
                    seen.add(nb)
                    q.append(p + [nb])
        return None

    def direct(a, b):
        return b in m.graph.get(a, [])

    ns = m.nodes()
    pairs = []
    for i in range(len(ns)):
        for j in range(i + 1, len(ns)):
            a, b = ns[i], ns[j]
            pairs.append({
                "a": a, "b": b,
                "reachable": m.reachable(a, b),
                "direct": direct(a, b),
                "path": path(a, b),
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