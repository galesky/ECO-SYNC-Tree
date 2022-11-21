from random import sample
import math

LIST_SIZE = 50
nodes = []
for i in range(LIST_SIZE):
  nodes.append(i)
print(sample(nodes, math.ceil(LIST_SIZE*0.87)))
print(sample(nodes, math.ceil(LIST_SIZE*0.17)))
print(sample(nodes, math.ceil(LIST_SIZE*0.55)))
print(sample(nodes, math.ceil(LIST_SIZE*0.35)))
