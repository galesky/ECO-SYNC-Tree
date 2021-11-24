import sys
import numpy as np
import matplotlib.pyplot as plt
from commonInformation import *


exp_name = sys.argv[1]
nodes = sys.argv[2]
protos = sys.argv[3]
payloads = sys.argv[4]
probs = sys.argv[5]
runs = sys.argv[6]

protos = protos.split(",")

dupes = {}
first_node_dead = 10000000
for proto in protos:
    first_node_cooldown = float(get_value_by_key(file_name.format(exp_name, nodes, proto, payloads, probs, runs), "FIRST_NODE_COOLDOWN"))
    catastrophe = float(get_value_by_key(file_name.format(exp_name, nodes, proto, payloads, probs, runs), "START_CATASTROPHE"))
    proto_first_node_dead = float(get_value_by_key(file_name.format(exp_name, nodes, proto, payloads, probs, runs), "FIRST_NODE_DEAD"))
    if proto_first_node_dead < first_node_dead:
        first_node_dead = proto_first_node_dead
    dupe_list = get_value_by_key(file_name.format(exp_name, nodes, proto, payloads, probs, runs), "TOTAL_DUPES_PER_SECOND").split(", ")
    dupes[proto] = list(map(float, dupe_list))

plt.rcParams.update({'font.size': 14})
fig = plt.figure(figsize=(10,5))
x = np.arange(len(dupe_list))
plt.xticks(np.arange(min(x), max(x)+1, 15))
plt.xlim(right=catastrophe + 150, left=catastrophe - 30)
plt.xlabel('Time (seconds)')
plt.ylabel('Duplicate Messages Received')
plt.axvline(catastrophe, color="grey", linestyle='--')

for proto in protos:
    plt.plot(x[:int(first_node_dead)], dupes[proto][:int(first_node_dead)], label=alg_mapper[proto], color=color_mapper[proto])

plt.tight_layout()
plt.legend()
plt.savefig(f'../plots/dupes_per_sec/dupes_per_second_cut_{exp_name}_{nodes}_{protos}_{payloads}_{probs}_{runs}.pdf', format='pdf')
plt.close(fig)
