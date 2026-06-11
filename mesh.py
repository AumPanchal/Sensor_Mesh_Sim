class Mesh:
    def __init__(self):
        #simple dict: node -> list of connected neighbors
        self.graph = {}

    def add_node(self, n):
        #create a node on its own, before wiring it to anything
        if n not in self.graph:
            self.graph[n] = []

    def connect(self, a, b):
        #set up empty lists if nodes are new
        self.add_node(a)
        self.add_node(b)

        #add each other to their lists
        if b not in self.graph[a]: self.graph[a].append(b)
        if a not in self.graph[b]: self.graph[b].append(a)

    def disconnect(self, a, b):
        #remove each other from their lists
        if b in self.graph.get(a, []): self.graph[a].remove(b)
        if a in self.graph.get(b, []): self.graph[b].remove(a)

    def reachable(self, a, b):
        #can a reach b through any path of connections
        if a == b:
            return True
        stack = [a]
        seen = set()
        while stack:
            cur = stack.pop()
            if cur == b:
                return True
            if cur in seen:
                continue
            seen.add(cur)
            for neighbor in self.graph.get(cur, []):
                stack.append(neighbor)
        return False

    def nodes(self):
        return sorted(self.graph.keys())

    def state(self):
        links = []
        for a in self.graph:
            for b in self.graph[a]:
                if a < b:
                    links.append((a, b))
        return {"nodes": self.nodes(), "links": links}