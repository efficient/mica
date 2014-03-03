#!/usr/bin/python

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
            f.write('server_thread,%s\n' % ' '.join([str(port_id) for port_id in thread]))
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
            f.write('workload_thread,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % (
                ' '.join([str(port_id) for port_id in thread[0]]),
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

    mac_addr = '80:00:00:00:00:{:02}'.format(addr_id)
    ip_addr = '10.0.0.{}'.format(addr_id)
    return mac_addr, ip_addr


class ConcurrencyModel:
    def concurrent_table_read(self, partition_id): pass
    def concurrent_table_write(self, partition_id): pass
    def concurrent_alloc_write(self, partition_id): pass
    def thread_id(self, partition_id): pass
    def hot_items(self): pass

class EREW(ConcurrencyModel):
    name = 'EREW'
    def concurrent_table_read(self, partition_id): return 0
    def concurrent_table_write(self, partition_id): return 0
    def concurrent_alloc_write(self, partition_id): return 0
    def thread_id(self, partition_id): return partition_id % 16
    def hot_items(self): return []

class CREW(EREW):
    name = 'CREW'
    def concurrent_table_read(self, partition_id): return 1

class CRCW(EREW):
    name = 'CRCW'
    def concurrent_table_read(self, partition_id): return 1
    def concurrent_table_write(self, partition_id): return 1

class CRCWS(EREW):
    name = 'CRCWS'
    def concurrent_table_read(self, partition_id): return 1
    def concurrent_table_write(self, partition_id): return 1
    def concurrent_alloc_write(self, partition_id): return 1

class CREW0(CREW):
    name = 'CREW0'
    def thread_id(self, partition_id): return 0     # all writes go to core 0

# use this for EREW partitions, CREW hot items
#class LB(EREW):
# use this for CREW partitions and hot items (uncomment MEHCACHED_LOAD_BALANCE_USE_CREW_PARTITION in netbench_analysis.c)
class LB(CREW):
    def __init__(self, num_hot_items, zipf, get_ratio):
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

    def thread_id(self, partition_id): return self.thread_id_list[partition_id]
    def hot_items(self): return self.hot_item_list


def main():
    datasets = [
            (8, 8, 192 * 1048576),
            (16, 64, 128 * 1048576),
            (128, 1024, 8 * 1048576),
        ]

    f = open('conf_prepopulation_empty', 'w')
    p = PrePopulationConf('server')
    p.set(0, 8, 8)
    p.write(f)

    for dataset, (key_length, value_length, num_items) in enumerate(datasets):
        assert key_length >= len('%x' % (num_items - 1))    # for hexadecimal key
        #num_partitions = 64
        num_partitions = 16
        # the following should be the same as in run_analysis_for_conf.py
        # isolated_server_numa_nodes = True
        isolated_server_numa_nodes = False

        # the followings are always 0 to allow exp.py to control duration
        load_duration = 0.
        trans_duration = 0.

        concurrency_list = [EREW(), CREW(), CRCW(), CRCWS(), CREW0()]
        for num_hot_items in (0, 32):
            for zipf in (('uniform', 0.), ('skewed', 0.99), ('single', 99.)):
                for get_ratio in (0., 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 1.):
                    concurrency_list.append(LB(num_hot_items, zipf, get_ratio))

        mth_threshold_list = (1.0, 0.5, 0.0)

        for concurrency in concurrency_list:
            for mth_threshold in mth_threshold_list:
                init_addr()

                f = open('conf_machines_%s_%s_%s' % (dataset, concurrency.name, mth_threshold), 'w')

                s = ServerConf('server')
                for port_id in range(8):
                    s.add_port(*next_addr())
                for thread_id in range(0, 16, 2):
                    s.add_thread(list(range(0, 4)))
                    s.add_thread(list(range(4, 8)))
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

                c0 = ClientConf('client0')
                for port in range(4):
                    c0.add_port(*next_addr())
                for thread_id in range(12):
                    c0.add_thread()
                c0.write(f)

                c1 = ClientConf('client1')
                for port in range(4):
                    c1.add_port(*next_addr())
                for thread_id in range(12):
                    c1.add_thread()
                c1.write(f)

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
            w = WorkloadConf(c0.client_name)
            for thread_id in range(12):
                if isolated_server_numa_nodes:
                    w.add_thread(list(range(4)), s.server_name, 0, num_items, key_length, value_length, zipf_theta, get_ratio, put_ratio, increment_ratio, load_batch_size, num_operations, duration)
                else:
                    w.add_thread(list(range(4)), s.server_name, -1, num_items, key_length, value_length, zipf_theta, get_ratio, put_ratio, increment_ratio, load_batch_size, num_operations, duration)
            w.write(f)
            w = WorkloadConf(c1.client_name)
            for thread_id in range(12):
                if isolated_server_numa_nodes:
                    w.add_thread(list(range(4)), s.server_name, 1, num_items, key_length, value_length, zipf_theta, get_ratio, put_ratio, increment_ratio, load_batch_size, num_operations, duration)
                else:
                    w.add_thread(list(range(4)), s.server_name, -1, num_items, key_length, value_length, zipf_theta, get_ratio, put_ratio, increment_ratio, load_batch_size, num_operations, duration)
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
                    w = WorkloadConf(c0.client_name)
                    for thread_id in range(12):
                        if isolated_server_numa_nodes:
                            w.add_thread(list(range(4)), s.server_name, 0, num_items, key_length, value_length, zipf_theta, get_ratio, put_ratio, increment_ratio, batch_size, num_operations, duration)
                        else:
                            w.add_thread(list(range(4)), s.server_name, -1, num_items, key_length, value_length, zipf_theta, get_ratio, put_ratio, increment_ratio, batch_size, num_operations, duration)
                    w.write(f)
                    w = WorkloadConf(c1.client_name)
                    for thread_id in range(12):
                        if isolated_server_numa_nodes:
                            w.add_thread(list(range(4)), s.server_name, 1, num_items, key_length, value_length, zipf_theta, get_ratio, put_ratio, increment_ratio, batch_size, num_operations, duration)
                        else:
                            w.add_thread(list(range(4)), s.server_name, -1, num_items, key_length, value_length, zipf_theta, get_ratio, put_ratio, increment_ratio, batch_size, num_operations, duration)
                    w.write(f)


if __name__ == '__main__':
    main()
