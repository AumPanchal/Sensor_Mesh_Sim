import json, os, urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer

NODE_ID = os.environ["NODE_ID"]
NODE_PORT = int(os.environ.get("NODE_PORT", 8000))

#neighbors this node knows about, set by orchestrator: {id -> weight}
neighbors = {}

def send_json(handler, body, code=200):
    raw = json.dumps(body).encode()
    handler.send_response(code)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Content-Length", str(len(raw)))
    handler.end_headers()
    handler.wfile.write(raw)

def ping(nid):
    #try to reach neighbor over Docker network, return True if up
    try:
        url = f"http://node_{nid.lower()}:{NODE_PORT}/ping"
        with urllib.request.urlopen(url, timeout=1) as r:
            return r.status == 200
    except:
        return False

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/ping":
            send_json(self, {"id": NODE_ID, "ok": True})
        else:
            self.send_error(404)

    def do_POST(self):
        global neighbors
        length = int(self.headers.get("Content-Length", 0))
        data = json.loads(self.rfile.read(length) or b"{}")

        if self.path == "/neighbors":
            #orchestrator is telling us who our neighbors are
            neighbors = {k: int(v) for k, v in data.get("weights", {}).items()}
            send_json(self, {"set": list(neighbors.keys())})

        elif self.path == "/reach":
            #ping each neighbor and report who's actually alive
            live = [nid for nid in neighbors if ping(nid)]
            send_json(self, {"id": NODE_ID, "live": live, "weights": neighbors})

        else:
            self.send_error(404)

    def log_message(self, *a):
        pass

if __name__ == "__main__":
    HTTPServer(("", NODE_PORT), Handler).serve_forever()