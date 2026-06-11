#http://localhost:6767

import json
import os
from collections import deque
from http.server import BaseHTTPRequestHandler, HTTPServer
from mesh import Mesh

PORT = 6767

#3 nodes start connected
mesh = Mesh()
for n in ("A", "B", "C"):
    mesh.add_node(n)
mesh.connect("A", "B")
mesh.connect("B", "C")
mesh.connect("A", "C")

#three possible links UI can toggle
CANDIDATE_EDGES = [("A", "B"), ("B", "C"), ("A", "C")]


def edge_up(a, b):
    return b in mesh.graph.get(a, [])


def find_path(a, b):
    #returns the actual route, ex. ["A", "B", "C"], or None
    if a == b:
        return [a]
    q = deque([[a]])
    seen = {a}
    while q:
        path = q.popleft()
        for nb in mesh.graph.get(path[-1], []):
            if nb == b:
                return path + [nb]
            if nb not in seen:
                seen.add(nb)
                q.append(path + [nb])
    return None


def state():
    edges = [{"a": a, "b": b, "up": edge_up(a, b)} for (a, b) in CANDIDATE_EDGES]
    pairs = []
    for (a, b) in CANDIDATE_EDGES:
        path = find_path(a, b)
        pairs.append({
            "a": a, "b": b,
            "reachable": mesh.reachable(a, b),
            "direct": edge_up(a, b),
            "path": path,
        })
    return {"nodes": mesh.nodes(), "edges": edges, "pairs": pairs}


class Handler(BaseHTTPRequestHandler):
    def _send(self, body, content_type="application/json", code=200):
        if isinstance(body, (dict, list)):
            body = json.dumps(body).encode()
        elif isinstance(body, str):
            body = body.encode()
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path in ("/", "/index.html"):
            path = os.path.join(os.path.dirname(__file__), "index.html")
            with open(path, "rb") as f:
                self._send(f.read(), "text/html")
        elif self.path == "/api/state":
            self._send(state())
        else:
            self.send_error(404)

    def do_POST(self):
        if self.path == "/api/toggle":
            length = int(self.headers.get("Content-Length", 0))
            data = json.loads(self.rfile.read(length) or "{}")
            a, b = data.get("a"), data.get("b")
            if (a, b) in CANDIDATE_EDGES or (b, a) in CANDIDATE_EDGES:
                if edge_up(a, b):
                    mesh.disconnect(a, b)
                else:
                    mesh.connect(a, b)
            self._send(state())
        else:
            self.send_error(404)

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    HTTPServer(("", PORT), Handler).serve_forever()