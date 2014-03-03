#!/usr/bin/python

import os

# isolated_server_numa_nodes = True
isolated_server_numa_nodes = False

for num_hot_items in (0, 32):
    for zipf in (('uniform', 0.), ('skewed', 0.99), ('single', 99.)):
        for get_ratio in (0., 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 1.):
            cmd = './netbench_analysis %d %f %f %d > analysis_%d_%s_%.2f' % (num_hot_items, zipf[1], get_ratio, isolated_server_numa_nodes, num_hot_items, zipf[0], get_ratio)
            os.system(cmd)

