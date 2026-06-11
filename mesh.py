class Mesh:
    def __init__(self):
        #simple dict: node -> list of connected neighbors
        self.graph ={}

    def connect(self, a, b):
        #setup empty lists if nodes are new
        if a not in self.graph: self.graph[a] =[]
        if b not in self.graph: self.graph[b] =[]
        
        #add each other to their lists
        if b not in self.graph[a]: self.graph[a].append(b)
        if a not in self.graph[b]: self.graph[b].append(a)

    def disconnect(self, a, b):
        #remove each other from their lists
        if b in self.graph.get(a, []): self.graph[a].remove(b)
        if a in self.graph.get(b, []): self.graph[b].remove(a)

    def reachable(self, a, b):
        #standard list instead of a fancy deque
        stack =[a]
        seen =[]
        
        while stack:
            cur =stack.pop()
            if cur ==b:
                return True
                
            if cur not in seen:
                seen.append(cur)
                #add all neighbors to the pile to check next
                for neighbor in self.graph.get(cur, []):
                    stack.append(neighbor)
                    
        return False

if __name__ =="__main__":
    m =Mesh()
    m.connect("A", "B")
    m.connect("B", "C")
    print("A reach C:", m.reachable("A", "C"))   #True
    m.disconnect("B", "C")                       #drop link
    print("A reach C:", m.reachable("A", "C"))   #False
    print("current state:", m.graph)