#!/usr/bin/python

import sys

class ServerConf:
    def __init__(self, server_name):
        self.server_name = server_name
        self.ports = []
        self.threads = []
        self.partitions = []
        self.hot_items = []

    def add_port(self, mac_addr, ip_addr):
        self.ports.append((mac_addr, ip_addr))

    def add_thread(self, port_ids):
        self.threads.append(port_ids)

    def add_partition(self, num_items, alloc_size, concurrent_table_read, concurrent_table_write, concurrent_alloc_write, thread_id, mth_threshold):
        self.partitions.append((num_items, alloc_size, concurrent_table_read, concurrent_table_write, concurrent_alloc_write, thread_id, mth_threshold))

    def add_hot_item(self, key_hash, thread_id):
        self.hot_items.append((key_hash, thread_id))

    def write(self, f):
        f.write('server,%s\n' % self.server_name)
        for port in self.ports:
            f.write('server_port,%s,%s\n' % port)
        for thread in self.threads:
            port_ids = ' '.join([str(port_id) for port_id in thread])
            if port_ids == '':
                port_ids = '-'
            f.write('server_thread,%s\n' % port_ids)
        for partition in self.partitions:
            f.write('server_partition,%s,%s,%s,%s,%s,%s,%s\n' % partition)
        for hot_item in self.hot_items:
            f.write('server_hot_item,%016x,%s\n' % hot_item)
        f.write('\n')

class ClientConf:
    def __init__(self, client_name):
        self.client_name = client_name
        self.ports = []
        self.threads = []

    def add_port(self, mac_addr, ip_addr):
        self.ports.append((mac_addr, ip_addr))

    def add_thread(self):
        self.threads.append(None)

    def write(self, f):
        f.write('client,%s\n' % self.client_name)
        for thread in self.threads:
            f.write('client_thread,\n')
        for port in self.ports:
            f.write('client_port,%s,%s\n' % port)
        f.write('\n')

class PrePopulationConf:
    def __init__(self, server_name):
        self.server_name = server_name
        self.dataset = None

    def set(self, num_items, key_length, value_length):
        self.dataset = (num_items, key_length, value_length)

    def write(self, f):
        f.write('prepopulation,%s\n' % self.server_name)
        f.write('dataset,%s,%s,%s\n' % self.dataset)
        f.write('\n')

class WorkloadConf:
    def __init__(self, client_name):
        self.client_name = client_name
        self.threads = []

    def add_thread(self, port_ids, server_name, partition_mode, num_items, key_length, value_length, zipf_theta, get_ratio, put_ratio, increment_ratio, batch_size, num_operations, duration):
        assert abs(get_ratio) + abs(put_ratio) + abs(increment_ratio) == 1.
        self.threads.append((port_ids, server_name, partition_mode, num_items, key_length, value_length, zipf_theta, get_ratio, put_ratio, increment_ratio, batch_size, num_operations, duration))

    def write(self, f):
        f.write('workload,%s\n' % self.client_name)
        for thread in self.threads:
            port_ids = ' '.join([str(port_id) for port_id in thread[0]])
            if port_ids == '':
                port_ids = '-'
            f.write('workload_thread,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % (
                port_ids,
                thread[1],
                thread[2],
                thread[3],
                thread[4],
                thread[5],
                thread[6],
                thread[7],
                thread[8],
                thread[9],
                thread[10],
                thread[11],
                thread[12]
                ))
        f.write('\n')

def init_addr():
    global _last_addr_id
    _last_addr_id = 0

def next_addr():
    global _last_addr_id

    addr_id = _last_addr_id
    _last_addr_id += 1

    mac_addr = '82:00:00:00:00:' + ('0' + hex(addr_id)[2:])[-2:]
    ip_addr = '10.0.0.' + str(addr_id)
    return mac_addr, ip_addr


class ConcurrencyModel:
    def __init__(self, partition_to_core_affinity):
        self.partition_to_core_affinity = partition_to_core_affinity
    def concurrent_table_read(self, partition_id): pass
    def concurrent_table_write(self, partition_id): pass
    def concurrent_alloc_write(self, partition_id): pass
    def thread_id(self, partition_id): pass
    def hot_items(self): pass

class EREW(ConcurrencyModel):
    name = 'EREW'
    def __init__(self, partition_to_core_affinity):
        ConcurrencyModel.__init__(self, partition_to_core_affinity)
    def concurrent_table_read(self, partition_id): return 0
    def concurrent_table_write(self, partition_id): return 0
    def concurrent_alloc_write(self, partition_id): return 0
    def thread_id(self, partition_id): return self.partition_to_core_affinity[partition_id]
    def hot_items(self): return []

class CREW(EREW):
    name = 'CREW'
    def __init__(self, partition_to_core_affinity):
        EREW.__init__(self, partition_to_core_affinity)
    def concurrent_table_read(self, partition_id): return 1

class CRCW(EREW):
    name = 'CRCW'
    def __init__(self, partition_to_core_affinity):
        EREW.__init__(self, partition_to_core_affinity)
    def concurrent_table_read(self, partition_id): return 1
    def concurrent_table_write(self, partition_id): return 1

class CRCWS(EREW):
    name = 'CRCWS'
    def __init__(self, partition_to_core_affinity):
        EREW.__init__(self, partition_to_core_affinity)
    def concurrent_table_read(self, partition_id): return 1
    def concurrent_table_write(self, partition_id): return 1
    def concurrent_alloc_write(self, partition_id): return 1

class CREW0(CREW):
    name = 'CREW0'
    def __init__(self, partition_to_core_affinity):
        CREW.__init__(self, partition_to_core_affinity)
    def thread_id(self, partition_id): return 0     # all writes go to core 0

# use this for EREW partitions, CREW hot items
#class LB(EREW):
# use this for CREW partitions and hot items (uncomment MEHCACHED_LOAD_BALANCE_USE_CREW_PARTITION in netbench_analysis.c)
class LB(CREW):
    def __init__(self, partition_to_core_affinity, num_hot_items, zipf, get_ratio):
        CREW.__init__(self, partition_to_core_affinity)

        self.name = 'LB-%d-%s-%.2f' % (num_hot_items, zipf[0], get_ratio)
        self.thread_id_list = None
        self.hot_item_list = None

        f = open('analysis_%d_%s_%.2f' % (num_hot_items, zipf[0], get_ratio))
        lines = list(f.readlines())
        i = 0
        while i < len(lines):
            line = lines[i]
            if line.strip() == 'partition_to_thread:':
                self.thread_id_list = eval('[' + lines[i + 1].strip() + ']')
            elif line.strip() == 'hot_item_to_thread:':
                self.hot_item_list = eval('[' + lines[i + 1].strip() + ']')
            i += 1
        assert self.thread_id_list != None
        assert self.hot_item_list != None

    def thread_id(self, partition_id): return self.thread_id_list[partition_id] # override the affinity
    def hot_items(self): return self.hot_item_list


def main(cluster, profiling_flag):
    datasets = [
            (8, 8, 192 * 1048576),
            (16, 64, 128 * 1048576),
            (128, 1024, 8 * 1048576),
            (250, 1152, 8 * 1048576),

            # half-size datasets (for when using only one domain of the server)
            (8, 8, 192 * 1048576 // 2),
            (16, 64, 128 * 1048576 // 2),
            (128, 1024, 8 * 1048576 // 2),
            (250, 1152, 8 * 1048576 //2),

        ]

    f = open('conf_prepopulation_empty', 'w')
    p = PrePopulationConf('server')
    p.set(0, 8, 8)
    p.write(f)

    num_client_machines = 10
    ib_memcached_address = ''

    if cluster == 'cmu-xia-routers':
        # CMU testbed (xia-router*)
        num_server_threads = 16
        num_server_ports = 8
        server_core_to_port_affinity = [[0, 1, 2, 3], [4, 5, 6, 7]] * 8

        num_partitions = 16
        partition_to_core_affinity = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]

        num_client_threads = 12
        num_client_ports = 4
        client_core_to_port_affinity = [[0, 1, 2, 3]] * 12

    elif cluster.startswith('emulab-c6220') and not cluster.startswith('emulab-c6220-ib'):
        # Emulab Apt (c6220)
        # for both Ethernet and IB
        num_server_threads = 16
        num_server_ports = 1

        core_count = int(cluster.split('-')[2])

        if core_count not in (8, 16):
            assert False, 'not support core count: %d' % core_count

        if core_count == 8:
            # NIC on node 0
            server_core_to_port_affinity = [[0]] * 8 + [[]] * 8
            # NIC on node 1
            #server_core_to_port_affinity = [[]] * 8 + [[0]] * 8
            num_partitions = 8
            # NIC on node 0
            partition_to_core_affinity = [0, 1, 2, 3, 4, 5, 6, 7]
            # NIC on node 1
            #partition_to_core_affinity = [8, 9, 10, 11, 12, 13, 14, 15]
        elif core_count == 16:
            server_core_to_port_affinity = [[0]] * 16
            num_partitions = 16
            partition_to_core_affinity = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]

        num_client_threads = 16
        num_client_ports = 1
        client_core_to_port_affinity = [[0]] * 16

    elif cluster.startswith('emulab-c6220-ib'):
        # Emulab Apt (c6220)
        # for both Ethernet and IB
        num_server_threads = 16
        num_server_ports = 1

        core_count = int(cluster.split('-')[3])

        if core_count not in (8, 16):
            assert False, 'not support core count: %d' % core_count

        if core_count == 8:
            # NIC on node 0
            server_core_to_port_affinity = [[0]] * 8 + [[]] * 8
            # NIC on node 1
            #server_core_to_port_affinity = [[]] * 8 + [[1]] * 8
            num_partitions = 8
            # NIC on node 0
            partition_to_core_affinity = [0, 1, 2, 3, 4, 5, 6, 7]
            # NIC on node 1
            #partition_to_core_affinity = [8, 9, 10, 11, 12, 13, 14, 15]
        elif core_count == 16:
            server_core_to_port_affinity = [[0]] * 16
            num_partitions = 16
            partition_to_core_affinity = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]

        num_client_threads = 16
        num_client_ports = 1
        client_core_to_port_affinity = [[0]] * 16

        ib_memcached_address = 'node-1.mica2.fawn.apt.emulab.net'

    elif cluster.startswith('intel-one-domain'):
        # Intel (two QDA1's on one domain for the server)
        core_count = int(cluster.split('-')[3])
        port_count = int(cluster.split('-')[4])

        if core_count not in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 20):
            assert False, 'not support core count: %d' % core_count
        if port_count not in (4, 8, 5, 6, 2, 1, 3):
            assert False, 'not support port count: %d' % port_count

        num_server_threads = 24
        # we need to use 8 ports always so that the client uses correct port mapping
        num_server_ports = 8
        if port_count == 4:
            ports = [0, 1, 2, 3]
        elif port_count == 8:
            ports = [0, 1, 2, 3] + [4, 5, 6, 7]
	elif port_count == 5:
	    ports = [0, 1, 2, 3] + [4]
	elif port_count == 6:
   	    ports = [0, 1, 2, 3] + [4, 5]
        elif port_count == 2:
            ports = [0, 1] 
        elif port_count == 3:
            ports = [0, 1, 2]
        elif port_count == 1:
              ports = [0]
	elif port_count == 7:
              ports = [0, 1, 2, 3] + [4, 5, 6]


        if core_count <= num_server_threads // 2:
            server_core_to_port_affinity =  [ports] * core_count + [[]] * (num_server_threads // 2 - core_count) + [[]]*(num_server_threads // 2)

            num_partitions = core_count
            partition_to_core_affinity = list(range(0, core_count))       # [10, ..., 10 + core_count - 1]
        elif core_count == 24:
            server_core_to_port_affinity = [ports] * num_server_threads

            num_partitions = num_server_threads
            partition_to_core_affinity = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11] + [12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23]

        num_client_threads = 24
        # we need to use 8 ports because port 4-7 are only accessible then
        num_client_ports = 8
        if port_count == 4:
            client_core_to_port_affinity = [[0, 1, 2, 3]] * num_client_threads
        elif port_count == 8:
            client_core_to_port_affinity = [[0, 1, 2, 3] + [4, 5, 6, 7]] * num_client_threads
	elif port_count == 5:
   	    client_core_to_port_affinity = [[0, 1, 2, 3] + [4]] * num_client_threads
	elif port_count == 6:
            client_core_to_port_affinity = [[0, 1, 2, 3] + [4, 5]] * num_client_threads
        elif port_count == 2:
            client_core_to_port_affinity = [[0, 1]] * num_client_threads
        elif port_count == 7:
            client_core_to_port_affinity = [[0, 1, 2, 3] + [4, 5, 6]] * num_client_threads
	elif port_count == 3:
            client_core_to_port_affinity = [[0, 1, 2] ] * num_client_threads
	elif port_count == 1:
            client_core_to_port_affinity = [[0]] * num_client_threads
	

    elif cluster.startswith('intel-two-domains'):
        # Intel (one QDA1 on each domain for the server)
        core_count = int(cluster.split('-')[3])
        port_count = int(cluster.split('-')[4])

        if core_count not in (2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38, 40):
            assert False, 'not support core count: %d' % core_count
        if port_count not in (2, 4, 6, 8, 9,  10, 12, 14, 15, 16):
            assert False, 'not support port count: %d' % port_count

        num_server_threads = 24

        if port_count == 2:
            num_server_ports = 16
            ports0 = [0]
            ports1 = [8]
        elif port_count == 4:
            num_server_ports = 16
            ports0 = [0, 1]
            ports1 = [8, 9]
        #elif port_count == 8:
        #    num_server_ports = 8
        #    ports0 = [0, 1, 2, 3]
        #    ports1 = [4, 5, 6, 7]
        elif port_count == 6:
            num_server_ports = 16
            ports0 = [0, 1, 2]
            ports1 = [8, 9, 10]
	elif port_count == 8:
            num_server_ports = 16
            ports0 = [0, 1, 2, 3]
            ports1 = [8, 9, 10, 11]
	elif port_count == 16:
            num_server_ports = 16
            ports0 = [0, 1, 2, 3, 4, 5, 6, 7]
            ports1 = [8, 9, 10, 11, 12, 13, 14, 15]
        elif port_count == 10:
            num_server_ports = 16
            ports0 = [0, 1, 2, 3, 4]
            ports1 = [8, 9, 10, 11, 12]
        elif port_count == 9:
            num_server_ports = 16
            ports0 = [0, 1, 2, 3, 4]
            ports1 = [8, 9, 10, 11]
        elif port_count == 12:
            num_server_ports = 16
            ports0 = [0, 1, 2, 3, 4, 5]
            ports1 = [8, 9, 10, 11, 12, 13]
        elif port_count == 14:
            num_server_ports = 16
            ports0 = [0, 1, 2, 3, 4, 5, 6]
            ports1 = [8, 9, 10, 11, 12, 13, 14]
        elif port_count == 15:
            num_server_ports = 16
            ports0 = [0, 1, 2, 3, 4, 5, 6, 7]
            ports1 = [8, 9, 10, 11, 12, 13, 14]

        # e.g., [ports0] * 1 + [[]] * 9 + [ports1] * 1 + [[]] * 9  for core_count == 2
        server_core_to_port_affinity = [ports0] * (core_count // 2) + [[]] * (num_server_threads // 2 - core_count // 2) + [ports1] * (core_count // 2) + [[]] * (num_server_threads // 2 - core_count // 2)
	
	num_partitions = core_count
        partition_to_core_affinity = list(range(0, core_count // 2)) + list(range(num_server_threads // 2, num_server_threads // 2 + core_count // 2))  # e.g., [0, 1] + [10, 11] for core_count == 2

        if (profiling_flag == "stap"): # When rx stats querying rate is low, having core 0 to particapate execution is still helpful (maybe removing core0 is good but removing core 10 at the same time offsets the benefits); so keep core0 for kvs and reserve first core (core 10) 
	        server_core_to_port_affinity =[[]] * 1 +  [ports0] * (core_count // 2 -1) + [[]] * (num_server_threads // 2 - core_count // 2 ) + [[]] * 1 + [ports1] * (core_count // 2-1) + [[]] * (num_server_threads // 2 - core_count // 2 )
		num_partitions = core_count - 1 
		partition_to_core_affinity = list(range(0, core_count // 2 )) + list(range(num_server_threads // 2 + 1, num_server_threads // 2 + core_count // 2 ))
		print('reserve frist core on NUMA socket 1 for profiling, total core count for KV processing is %s' % num_partitions)
	# the only configuration possible because a client thread must be able to send requests to both server domains
        # this is unlike cmu-xia-routers where a domain has two NICs, each of which is connected to different server domains
        num_client_threads = 24
        if port_count == 2:
            num_client_ports = 16
            client_core_to_port_affinity = [[0] + [8]] * num_client_threads
        #elif port_count == 8:
        #    num_client_ports = 8
        #    client_core_to_port_affinity = [[0, 1, 2, 3] + [4, 5, 6, 7]] * num_client_threads
        elif port_count == 4:
            num_client_ports = 16
            client_core_to_port_affinity = [ [0, 1] + [8, 9] ] * num_client_threads
        elif port_count == 6:
            num_client_ports = 16
            client_core_to_port_affinity = [ [0, 1, 2] + [8, 9, 10] ] * num_client_threads
        elif port_count == 8:
            num_client_ports = 16
            #client_core_to_port_affinity = [ [0, 1, 2, 3] + [5, 6, 7, 8] ] * 20
	    #client_core_to_port_affinity = [ [8, 9, 10, 11] + [12, 13, 14, 15] ] * 20
	    client_core_to_port_affinity = [ [0, 1, 2, 3] + [8, 9, 10, 11] ] * num_client_threads
	elif port_count == 16: 
            num_client_ports = 16
            client_core_to_port_affinity = [[0, 1, 2, 3, 4, 5, 6, 7] + [8, 9, 10, 11, 12, 13, 14, 15]] * num_client_threads
        elif port_count == 10:
            num_client_ports = 16
            client_core_to_port_affinity = [[0, 1, 2, 3, 4] + [8, 9, 10, 11, 12]] * num_client_threads
        elif port_count == 9:
            num_client_ports = 16
            client_core_to_port_affinity = [[0, 1, 2, 3] + [8, 9, 10, 11, 12]] * num_client_threads
        elif port_count == 12:
            num_client_ports = 16
            client_core_to_port_affinity = [[0, 1, 2, 3, 4, 5] + [8, 9, 10, 11, 12, 13]] * num_client_threads
        elif port_count == 14:
            num_client_ports = 16
            client_core_to_port_affinity = [[0, 1, 2, 3, 4, 5, 6] + [8, 9, 10, 11, 12, 13, 14]] * num_client_threads
        elif port_count == 15:
            num_client_ports = 16
            client_core_to_port_affinity = [[0, 1, 2, 3, 4, 5, 6, 7] + [8, 9, 10, 11, 12, 13, 14]] * num_client_threads

    elif cluster.startswith('intel-direct-one-domain'):
        # Intel (two QDA1's on one domain for the server)
        core_count = int(cluster.split('-')[4])
        port_count = int(cluster.split('-')[5])

        if core_count not in (4, 8):
            assert False, 'not support core count: %d' % core_count
        if port_count not in (4, 8) or core_count != port_count:
            assert False, 'not support port count: %d' % port_count

        num_server_threads = 20
        num_server_ports = 8

        num_client_threads = 20
        num_client_ports = 8

        if core_count == 4:
            server_core_to_port_affinity = [[]] * 10 + [[0], [1], [2], [3]] + [[]] * 6

            num_partitions = 4
            partition_to_core_affinity = [10, 11, 12, 13]

            client_core_to_port_affinity = [[4, 5, 6, 7]] * 20
        elif core_count == 8:
            server_core_to_port_affinity = [[]] * 10 + [[0], [1], [2], [3], [4], [5], [6], [7]] + [[]] * 2

            num_partitions = 8
            partition_to_core_affinity = [10, 11, 12, 13, 14, 15, 16, 17]

            client_core_to_port_affinity = [[0, 1, 2, 3] + [4, 5, 6, 7]] * 20

    elif cluster.startswith('intel-direct-two-domains'):
        # Intel (one QDA1 on each domain for the server)
        core_count = int(cluster.split('-')[4])
        port_count = int(cluster.split('-')[5])

        if core_count not in (4, 8, 16):
            assert False, 'not support core count: %d' % core_count
        if port_count not in (4, 8, 16) or core_count != port_count:
            assert False, 'not support port count: %d' % port_count

        num_server_threads = 20

        num_client_threads = 20

        if core_count == 4:
            num_server_ports = 8
            server_core_to_port_affinity = [[0], [1]] + [[]] * 8 + [[4], [5]] + [[]] * 8

            num_partitions = 4
            partition_to_core_affinity = [0, 1] + [10, 11]

            num_client_ports = 8
            client_core_to_port_affinity = [[0, 1] + [4, 5]] * 20
        elif core_count == 8:
            num_server_ports = 8
            server_core_to_port_affinity = [[0], [1], [2], [3]] + [[]] * 6 + [[4], [5], [6], [7]] + [[]] * 6

            num_partitions = 8
            partition_to_core_affinity = [0, 1, 2, 3] + [10, 11, 12, 13]

            num_client_ports = 8
            client_core_to_port_affinity = [[0, 1, 2, 3] + [4, 5, 6, 7]] * 20
        elif core_count == 16:
            num_server_ports = 16
            server_core_to_port_affinity = [[0], [1], [2], [3], [4], [5], [6], [7]] + [[]] * 2 + [[8], [9], [10], [11], [12], [13], [14], [15]] + [[]] * 2

            num_partitions = 16
            partition_to_core_affinity = [0, 1, 2, 3, 4, 5, 6, 7] + [10, 11, 12, 13, 14, 15, 16, 17]

            num_client_ports = 16
            client_core_to_port_affinity = [[0, 1, 2, 3, 4, 5, 6, 7] + [8, 9, 10, 11, 12, 13, 14, 15]] * 20

    elif cluster.startswith('intel-two-to-one'):
        core_count = int(cluster.split('-')[4])
        port_count = int(cluster.split('-')[5])

        assert 1 <= core_count <= 24
        assert core_count % 4 == 0
        assert core_count == port_count * 2

        num_server_threads = 24
        num_server_ports = 16

        server_core_to_port_affinity = [[]] * num_server_threads
        active_port_ids = set()

        num_partitions = core_count
        partition_to_core_affinity = []
	shared_port_ids = []

        node_0_core_offset = 0
        node_0_port_offset = 0
       
        for reminder_port_id in range(core_count // 2 //2, port_count //2):
	    shared_port_ids.append(reminder_port_id) 
	    active_port_ids.add(reminder_port_id)
        #print(shared_port_ids)

	for core_id in range(node_0_core_offset, node_0_core_offset + core_count // 2):
            port_id = node_0_port_offset + (core_id - node_0_core_offset) // 2
	    server_core_to_port_affinity[core_id] = [port_id] + shared_port_ids
	 #   print( server_core_to_port_affinity)
            active_port_ids.add(port_id)
            partition_to_core_affinity.append(core_id)
	  #  print( partition_to_core_affinity)


        node_1_core_offset = num_server_threads // 2
        node_1_port_offset = num_server_ports // 2

       	shared_port_ids = []

	for reminder_port_id in range(core_count // 2 //2, port_count //2):
	    shared_port_ids.append(reminder_port_id+node_1_port_offset) 
	    active_port_ids.add(reminder_port_id+node_1_port_offset)
        #print(shared_port_ids)


	for core_id in range(node_1_core_offset, node_1_core_offset + core_count // 2):
            port_id = node_1_port_offset + (core_id - node_1_core_offset) // 2
            server_core_to_port_affinity[core_id] = [port_id] + shared_port_ids
         #   print( server_core_to_port_affinity)
	    active_port_ids.add(port_id)
            partition_to_core_affinity.append(core_id)
          #  print( partition_to_core_affinity)


        active_port_ids = list(sorted(active_port_ids))

        num_client_threads = 24
        num_client_ports = 16
        client_core_to_port_affinity = [active_port_ids] * num_client_threads

#	if port_count == 14:
#            num_client_ports = 16
#            client_core_to_port_affinity = [[0, 1, 2, 3, 4, 5, 6] + [8, 9, 10, 11, 12, 13, 14]] * num_client_threads
#        elif port_count == 16:
#            num_client_ports = 16
#            client_core_to_port_affinity = [[0, 1, 2, 3, 4, 5, 6, 7] + [8, 9, 10, 11, 12, 13, 14, 15]] * num_client_threads

    elif cluster.startswith('intel-ib-one-domain'):
        # Intel (one IB on one domain)
        core_count = int(cluster.split('-')[4])
        port_count = int(cluster.split('-')[5])

        if core_count not in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20):
            assert False, 'not support core count: %d' % core_count
        if port_count not in (1, 2, 3, 4):
            assert False, 'not support port count: %d' % port_count

        num_server_threads = 20
        num_server_ports = 4
        ports = list(range(0, port_count))      # [0, ..., port_count - 1]

        if core_count <= 10:
            server_core_to_port_affinity = [[]] * 10 + [ports] * core_count + [[]] * (10 - core_count)

            num_partitions = core_count
            partition_to_core_affinity = list(range(10, 10 + core_count))       # [10, ..., 10 + core_count - 1]
        elif core_count == 20:
            server_core_to_port_affinity = [ports] * 20

            num_partitions = 20
            partition_to_core_affinity = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9] + [10, 11, 12, 13, 14, 15, 16, 17, 18, 19]

        # clients always use 1 port
        num_client_threads = 20
        num_client_ports = 1
        client_core_to_port_affinity = [[0]] * 20

        ib_memcached_address = 'node-1.mica.fawn.apt.emulab.net'

    elif cluster.startswith('intel-ib-two-domains'):
        # Intel (one IB on each domain)
        core_count = int(cluster.split('-')[4])
        port_count = int(cluster.split('-')[5])

        if core_count not in (2, 4, 6, 8, 10, 12, 14, 16, 18, 20):
            assert False, 'not support core count: %d' % core_count
        if port_count not in (1, 2, 3, 4):
            assert False, 'not support port count: %d' % port_count

        num_server_threads = 20
        num_server_ports = 4
        if port_count == 1:
            ports0 = [0]
            ports1 = [0]
        elif port_count == 2:
            ports0 = [0]
            ports1 = [2]
        elif port_count == 3:
            ports0 = [0, 1]
            ports1 = [2]
        elif port_count == 4:
            ports0 = [0, 1]
            ports1 = [2, 3]

        # e.g., [ports0] * 1 + [[]] * 9 + [ports1] * 1 + [[]] * 9  for core_count == 2
        server_core_to_port_affinity = [ports0] * (core_count // 2) + [[]] * (10 - core_count // 2) + [ports1] * (core_count // 2) + [[]] * (10 - core_count // 2)

        num_partitions = core_count
        partition_to_core_affinity = list(range(0, core_count // 2)) + list(range(10, 10 + core_count // 2))  # e.g., [0, 1] + [10, 11] for core_count == 2

        # clients always use 1 port
        num_client_threads = 20
        num_client_ports = 1
        client_core_to_port_affinity = [[0]] * 20

        ib_memcached_address = 'node-1.mica.fawn.apt.emulab.net'

    else:
        assert False, 'unrecognized cluster: %s' % cluster

    open('conf_ib_memcached_address', 'wt').write(ib_memcached_address)

    for dataset, (key_length, value_length, num_items) in enumerate(datasets):
        assert key_length >= len('%x' % (num_items - 1))    # for hexadecimal key
        # the following should be the same as in run_analysis_for_conf.py
        # isolated_server_numa_nodes = True
        isolated_server_numa_nodes = False

        # the followings are always 0 to allow exp.py to control duration
        load_duration = 0.
        trans_duration = 0.

        concurrency_list = [EREW(partition_to_core_affinity), CREW(partition_to_core_affinity), CRCW(partition_to_core_affinity), CRCWS(partition_to_core_affinity), CREW0(partition_to_core_affinity)]
        #for num_hot_items in (0, 32):
        #    for zipf in (('uniform', 0.), ('skewed', 0.99), ('single', 99.)):
        #        for get_ratio in (0., 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 1.):
        #            concurrency_list.append(LB(partition_to_core_affinity, num_hot_items, zipf, get_ratio))

        mth_threshold_list = (1.0, 0.5, 0.0)

        for concurrency in concurrency_list:
            for mth_threshold in mth_threshold_list:
                init_addr()

                f = open('conf_machines_%s_%s_%s' % (dataset, concurrency.name, mth_threshold), 'w')

                s = ServerConf('server')
                for port_id in range(num_server_ports):
                    s.add_port(*next_addr())
                for thread_id in range(0, num_server_threads):
                    s.add_thread(server_core_to_port_affinity[thread_id])
                for partition_id in range(num_partitions):
                    num_items_per_partition = num_items / num_partitions
                    alloc_size_per_partition = num_items * (key_length + value_length) / num_partitions

                    concurrent_table_read = concurrency.concurrent_table_read(partition_id)
                    concurrent_table_write = concurrency.concurrent_table_write(partition_id)
                    concurrent_alloc_write = concurrency.concurrent_alloc_write(partition_id)
                    thread_id = concurrency.thread_id(partition_id)
                    s.add_partition(num_items_per_partition, alloc_size_per_partition, concurrent_table_read, concurrent_table_write, concurrent_alloc_write, thread_id, mth_threshold)
                for hot_item in concurrency.hot_items():
                    s.add_hot_item(*hot_item)
                s.write(f)

                cs = []
                for i in range(num_client_machines):
                    c = ClientConf('client%d' % i)
                    for port in range(num_client_ports):
                        c.add_port(*next_addr())
                    for thread_id in range(num_client_threads):
                        c.add_thread()
                    c.write(f)
                    cs.append(c)

        f = open('conf_prepopulation_%s' % dataset, 'w')
        p = PrePopulationConf('server')
        p.set(num_items, key_length, value_length)
        p.write(f)

        for zipf in (('uniform', 0.), ('skewed', 0.99), ('single', 99.)):
            # load operations
            f = open('conf_workload_%s_load_%s' % (dataset, zipf[0]), 'w')
            if zipf[1] == 0.:
                # use sequential uniform instead for fast ingest
                zipf_theta = -1.0
            else:
                # other skewed distributions usually allow fast ingest
                zipf_theta = zipf[1]
            get_ratio = 0.
            put_ratio = 1. - get_ratio
            increment_ratio = 0.
            load_batch_size = 1
            num_operations = 0
            duration = load_duration
            for i in range(num_client_machines):
                c = cs[i]
                w = WorkloadConf(c.client_name)
                for thread_id in range(num_client_threads):
                    ports = client_core_to_port_affinity[thread_id]
                    if isolated_server_numa_nodes:
                        w.add_thread(ports, s.server_name, 0, num_items, key_length, value_length, zipf_theta, get_ratio, put_ratio, increment_ratio, load_batch_size, num_operations, duration)
                    else:
                        w.add_thread(ports, s.server_name, -1, num_items, key_length, value_length, zipf_theta, get_ratio, put_ratio, increment_ratio, load_batch_size, num_operations, duration)
                w.write(f)

            # trans operations
            zipf_theta = zipf[1]
            for get_ratio, put_ratio, increment_ratio in (
                    (0., 1., 0.),
                    (0.1, 0.9, 0.),
                    (0.25, 0.75, 0.),
                    (0.5, 0.5, 0.),
                    (0.75, 0.25, 0.),
                    (0.9, 0.1, 0.),
                    (0.95, 0.05, 0.),
                    (0.99, 0.01, 0.),
                    (1., 0., 0.),
                    (0., -1., 0.),
                    (-0.1, -0.9, 0.),
                    (-0.25, -0.75, 0.),
                    (-0.5, -0.5, 0.),
                    (-0.75, -0.25, 0.),
                    (-0.9, -0.1, 0.),
                    (-0.95, -0.05, 0.),
                    (-0.99, -0.01, 0.),
                    (-1., 0., 0.),
                    (0., 0., 1.),
                ):
                for batch_size in (1, 2, 4, 8, 16, 32):
                    f = open('conf_workload_%s_%s_%.2f_%.2f_%.2f_%s' % (dataset, zipf[0], get_ratio, put_ratio, increment_ratio, batch_size), 'w')
                    num_operations = 0
                    duration = trans_duration
                    for i in range(num_client_machines):
                        c = cs[i]
                        w = WorkloadConf(c.client_name)
                        for thread_id in range(num_client_threads):
                            ports = client_core_to_port_affinity[thread_id]
                            if isolated_server_numa_nodes:
                                w.add_thread(ports, s.server_name, 0, num_items, key_length, value_length, zipf_theta, get_ratio, put_ratio, increment_ratio, batch_size, num_operations, duration)
                            else:
                                w.add_thread(ports, s.server_name, -1, num_items, key_length, value_length, zipf_theta, get_ratio, put_ratio, increment_ratio, batch_size, num_operations, duration)
                        w.write(f)


if __name__ == '__main__':
    #if (len(sys.argv) != 3) and (len(sys.argv) != 2):
    if (len(sys.argv) not in [1, 2, 3]):
        print('%s CLUSTER <optional profiling_flag (stap)>' % sys.argv[0] ) 
        print('CLUSTER:')
        print('cmu-xia-routers')
        print('emulab-c6220-CORE                   CORE: 8, 16')
        print('intel-one-domain-CORE-PORT          CORE: 1, 2, ..., 9, 10, 20   PORT: 4, 8')
        print('intel-two-domains-CORE-PORT         CORE: 2, 4, ..., 18, 20      PORT: 4, 8, 16')
        print('intel-direct-one-domain-CORE-PORT   CORE: 4, 8                   PORT: 4, 8     (=CORE)')
        print('intel-direct-two-domains-CORE-PORT  CORE: 4, 8, 16               PORT: 4, 8, 16 (=CORE)')
        print('intel-two-to-one-CORE-PORT          CORE: 4, 8, ..., 16, 20      PORT: 2, ..., 10 (=CORE/2)')
        sys.exit(1)

    if (len(sys.argv) == 1):
        cluster = 'intel-two-to-one-24-12'
    else:
        cluster = sys.argv[1]
    
    if (len(sys.argv) == 3):
	profiling_flag = sys.argv[2]
    else:
         profiling_flag = ''
    main(cluster, profiling_flag)


