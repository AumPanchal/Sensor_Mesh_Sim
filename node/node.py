import json, os, urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer
from collections import deque

NODE_ID = os.environ["NODE_ID"]
NODE_PORT = int(os.environ.get("NODE_PORT", 8000))

# Neighbors this node knows about: {id -> weight}
neighbors = {}

# Transient buffer: holds the last 5 events to prevent memory bloat
recent_messages = deque(maxlen=5)

def send_json(handler, body, code=200):
    raw = json.dumps(body).encode()
    handler.send_response(code)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Content-Length", str(len(raw)))
    handler.end_headers()
    handler.wfile.write(raw)

def ping(nid):
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
        elif self.path == "/traffic":
            # Expose ephemeral logs for the UI visualization
            send_json(self, {"id": NODE_ID, "recent": list(recent_messages)})
        else:
            self.send_error(404)

    def do_POST(self):
        global neighbors
        length = int(self.headers.get("Content-Length", 0))
        data = json.loads(self.rfile.read(length) or b"{}")

        if self.path == "/neighbors":
            neighbors = {k: int(v) for k, v in data.get("weights", {}).items()}
            send_json(self, {"set": list(neighbors.keys())})

        elif self.path == "/reach":
            live = [nid for nid in neighbors if ping(nid)]
            send_json(self, {"id": NODE_ID, "live": live, "weights": neighbors})

        elif self.path == "/message":
            msg_id = data.get("msg_id", "unknown")
            path = data.get("path", [])
            
            # Check if this node is the source (the first node in the path)
            if path and path[0] == NODE_ID:
                recent_messages.append({"id": msg_id, "action": "generated"})
            else:
                recent_messages.append({"id": msg_id, "action": "received"})

            # If we are not the final destination, pop ourselves and forward
            if path and path[-1] != NODE_ID:
                try:
                    next_idx = path.index(NODE_ID) + 1
                    next_node = path[next_idx]
                    
                    url = f"http://node_{next_node.lower()}:{NODE_PORT}/message"
                    req = urllib.request.Request(url, data=json.dumps(data).encode(), headers={"Content-Type": "application/json"})
                    urllib.request.urlopen(req, timeout=1)
                    
                    recent_messages.append({"id": msg_id, "action": "forwarded_to_" + next_node})
                except Exception as e:
                    # If the connection fails, it logs dropped
                    recent_messages.append({"id": msg_id, "action": "dropped"})
            
            send_json(self, {"status": "ok"})

        else:
            self.send_error(404)

    def log_message(self, *a):
        pass

if __name__ == "__main__":
    HTTPServer(("", NODE_PORT), Handler).serve_forever()