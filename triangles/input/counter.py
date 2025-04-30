#!/usr/bin/env python3

import networkx as nx
import sys 

G = nx.Graph()
with open (sys.argv[1], 'r') as f:
    for line in f:
        a, b = line.split()
        G.add_edge(a, b)
num = 0
D = nx.triangles(G)
for v in D.values():
    num += v 
print(f"Number of triangles: {num / 3}")