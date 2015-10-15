// Copyright 2014 Carnegie Mellon University
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <assert.h>

#include "mehcached.h"
#include "hash.h"
#include "zipf.h"
#include "perf_count/perf_count.h"

#include "netbench_config.h"

#include <rte_launch.h>
#include <rte_eal.h>
#include <rte_lcore.h>
#include <rte_log.h>
#include <rte_debug.h>

//#define USE_PERF_COUNT
#define USE_PREFETCHING
#define PREFETCH_GAP (1)
#define PREFETCH_PIPELINE (PREFETCH_GAP * 2)

// #ifdef NDEBUG
// #undef NDEBUG
// #endif

enum PERF_COUNT_TYPE pct[4];
size_t pct_size = sizeof(pct) / sizeof(pct[0]);

perf_count_t
benchmark_perf_count_init()
{
#ifdef USE_PERF_COUNT
    assert(pct_size == 4);
    pct[0] = perf_count_type_by_name("BranchInstructions");
    pct[1] = perf_count_type_by_name("BranchMisses");
    pct[2] = perf_count_type_by_name("CacheReferences");
    pct[3] = perf_count_type_by_name("CacheMisses");
    perf_count_t pc = perf_count_init(pct, pct_size, 0);
    return pc;
#endif
    return NULL;
}

void
benchmark_perf_count_free(perf_count_t pc)
{
#ifdef USE_PERF_COUNT
    perf_count_free(pc);
#else
    (void)pc;
#endif
}

void
benchmark_perf_count_start(perf_count_t pc)
{
#ifdef USE_PERF_COUNT
    perf_count_start(pc);
#else
    (void)pc;
#endif
}

void
benchmark_perf_count_stop(perf_count_t pc)
{
#ifdef USE_PERF_COUNT
    perf_count_stop(pc);
    size_t i;
    for (i = 0; i < pct_size; i++)
        printf("%-20s: %10lu\n", perf_count_name_by_type(pct[i]), perf_count_get_by_index(pc, i));
    perf_count_reset(pc);
#else
    (void)pc;
#endif
}

static size_t running_threads;

typedef enum _benchmark_mode_t
{
    BENCHMARK_MODE_ADD = 0,
    BENCHMARK_MODE_SET,
    BENCHMARK_MODE_GET_HIT,
    BENCHMARK_MODE_GET_MISS,
    BENCHMARK_MODE_GET_SET_95,
    BENCHMARK_MODE_GET_SET_50,
    BENCHMARK_MODE_DELETE,
    BENCHMARK_MODE_SET_1,
    BENCHMARK_MODE_GET_1,
    BENCHMARK_MODE_MAX,
} benchmark_mode_t;

typedef enum _concurrency_mode_t
{
    CONCURRENCY_MODE_EREW = 0,
    CONCURRENCY_MODE_CREW,
    CONCURRENCY_MODE_CRCW,  // not supported yet
    CONCURRENCY_MODE_CRCWS,
} concurrency_mode_t;

struct proc_arg
{
    size_t num_threads;

    size_t key_length;
    size_t value_length;

    size_t op_count;
    uint8_t *op_types;
    uint8_t *op_keys;
    uint64_t *op_key_hashes;
    uint16_t *op_key_parts;
    uint8_t *op_values;

    size_t num_partitions;
    struct mehcached_table *tables[MEHCACHED_MAX_PARTITIONS];

    size_t owner_thread_id[MEHCACHED_MAX_PARTITIONS];

    benchmark_mode_t benchmark_mode;
    concurrency_mode_t concurrency_mode;

    uint64_t junk;
    uint64_t success_count;
    // size_t num_existing_items;
    // size_t *existing_items;
};

static
uint16_t
mehcached_get_partition_id(uint64_t key_hash, uint16_t num_partitions)
{
    //return (uint16_t)(key_hash >> 48) & (uint16_t)(num_partitions - 1);
	// for non power-of-two num_partitions
    return (uint16_t)(key_hash >> 48) % (uint16_t)num_partitions;
}

int
benchmark_proc(void *arg)
{
    struct proc_arg *p_arg = (struct proc_arg *)arg;

    uint8_t thread_id = (uint8_t)rte_lcore_id();
    const size_t num_threads = p_arg->num_threads;

    const size_t key_length = p_arg->key_length;
    const size_t value_length = p_arg->value_length;

    const int64_t op_count = (int64_t)p_arg->op_count;
    const uint8_t *op_types = p_arg->op_types;
    const uint8_t *op_keys = p_arg->op_keys;
    const uint64_t *op_key_hashes = p_arg->op_key_hashes;
    const uint16_t *op_key_parts = p_arg->op_key_parts;
    const uint8_t *op_values = p_arg->op_values;

    //const uint16_t num_partitions = p_arg->num_partitions;
    struct mehcached_table **tables = p_arg->tables;

    size_t *owner_thread_id = p_arg->owner_thread_id;

    benchmark_mode_t benchmark_mode = p_arg->benchmark_mode;
    concurrency_mode_t concurrency_mode = p_arg->concurrency_mode;

    uint64_t junk = 0;
    uint64_t success_count = 0;

    int64_t i;

#ifdef USE_PREFETCHING
    struct mehcached_prefetch_state prefetch_state[PREFETCH_PIPELINE];
#endif

    __sync_add_and_fetch((volatile size_t *)&running_threads, 1);
    while (*(volatile size_t *)&running_threads < num_threads)
        ;

    uint8_t alloc_id = 0;   // TODO: support CRCW

    switch (benchmark_mode)
    {
        case BENCHMARK_MODE_ADD:
            for (i = -PREFETCH_GAP * 2; i < op_count; i++)
            {
#ifdef USE_PREFETCHING
                if (i + PREFETCH_GAP * 2 < op_count)
                {
                    struct mehcached_table *table = tables[op_key_parts[i + PREFETCH_GAP * 2]];
                    mehcached_prefetch_table(table, op_key_hashes[i + PREFETCH_GAP * 2], &prefetch_state[(i + PREFETCH_GAP * 2) % PREFETCH_PIPELINE]);
                }
                if (i + PREFETCH_GAP >= 0 && i + PREFETCH_GAP < op_count)
                    mehcached_prefetch_alloc(&prefetch_state[(i + PREFETCH_GAP) % PREFETCH_PIPELINE]);
#endif
                if (i >= 0)
                {
                    struct mehcached_table *table = tables[op_key_parts[i]];
                    if (mehcached_set(alloc_id, table, op_key_hashes[i], op_keys + (size_t)i * key_length, key_length, op_values + (size_t)i * value_length, value_length, 0, false))
                        success_count++;
                }
            }
            break;
        case BENCHMARK_MODE_SET:
        case BENCHMARK_MODE_GET_HIT:
        case BENCHMARK_MODE_GET_MISS:
        case BENCHMARK_MODE_GET_SET_95:
        case BENCHMARK_MODE_GET_SET_50:
        case BENCHMARK_MODE_SET_1:
        case BENCHMARK_MODE_GET_1:
            for (i = -PREFETCH_GAP * 2; i < op_count; i++)
            {
#ifdef USE_PREFETCHING
                if (i + PREFETCH_GAP * 2 < op_count)
                {
                    struct mehcached_table *table = tables[op_key_parts[i + PREFETCH_GAP * 2]];
                    mehcached_prefetch_table(table, op_key_hashes[i + PREFETCH_GAP * 2], &prefetch_state[(i + PREFETCH_GAP * 2) % PREFETCH_PIPELINE]);
                }
                if (i + PREFETCH_GAP >= 0 && i + PREFETCH_GAP < op_count)
                    mehcached_prefetch_alloc(&prefetch_state[(i + PREFETCH_GAP) % PREFETCH_PIPELINE]);
#endif
                if (i >= 0)
                {
                    struct mehcached_table *table = tables[op_key_parts[i]];
                    bool is_get = op_types[i] == 0;
                    if (is_get)
                    {
                        uint8_t value[value_length];
                        size_t value_length = value_length;
						bool readonly;
						if (thread_id == owner_thread_id[op_key_parts[i]])
							readonly = false;
						else if (concurrency_mode >= CONCURRENCY_MODE_CRCW) // concurrent_table_write
							readonly = false;
						else
							readonly = true;
                        if (mehcached_get(alloc_id, table, op_key_hashes[i], op_keys + (size_t)i * key_length, key_length, value, &value_length, NULL, readonly))
                            success_count++;
                        junk += (uint64_t)value[0];
                    }
                    else
                    {
                        if (mehcached_set(alloc_id, table, op_key_hashes[i], op_keys + (size_t)i * key_length, key_length, op_values + (size_t)i * value_length, value_length, 0, true))
                            success_count++;
                    }
                }
            }
            break;
        case BENCHMARK_MODE_DELETE:
            for (i = -PREFETCH_GAP * 2; i < op_count; i++)
            {
#ifdef USE_PREFETCHING
                if (i + PREFETCH_GAP * 2 < op_count)
                {
                    struct mehcached_table *table = tables[op_key_parts[i + PREFETCH_GAP * 2]];
                    mehcached_prefetch_table(table, op_key_hashes[i + PREFETCH_GAP * 2], &prefetch_state[(i + PREFETCH_GAP * 2) % PREFETCH_PIPELINE]);
                }
                if (i + PREFETCH_GAP >= 0 && i + PREFETCH_GAP < op_count)
                    mehcached_prefetch_alloc(&prefetch_state[(i + PREFETCH_GAP) % PREFETCH_PIPELINE]);
#endif
                if (i >= 0)
                {
                    struct mehcached_table *table = tables[op_key_parts[i]];
                    if (mehcached_delete(alloc_id, table, op_key_hashes[i], op_keys + (size_t)i * key_length, key_length))
                        success_count++;
                }
            }
            break;
        default:
            assert(false);
    }

    p_arg->junk = junk;
    p_arg->success_count = success_count;

	return 0;
}

void
benchmark(const concurrency_mode_t concurrency_mode, double zipf_theta, double mth_threshold)
{
    printf("benchmark\n");

    printf("zipf_theta = %lf\n", zipf_theta);

    switch (concurrency_mode)
    {
        case CONCURRENCY_MODE_EREW:
            printf("concurrency_mode = EREW\n");
            break;
        case CONCURRENCY_MODE_CREW:
            printf("concurrency_mode = CREW\n");
            break;
        case CONCURRENCY_MODE_CRCW:
            printf("concurrency_mode = CRCW\n");
            break;
        case CONCURRENCY_MODE_CRCWS:
            printf("concurrency_mode = CRCWS\n");
            break;
        default:
            assert(false);
    }

    const size_t num_items = 16 * 1048576;
    const size_t num_partitions = 16;

    const size_t num_threads = 16;
    const size_t num_operations = 16 * 1048576;
    const size_t max_num_operations_per_thread = num_operations;

    const size_t key_length = MEHCACHED_ROUNDUP8(8);
    const size_t value_length = MEHCACHED_ROUNDUP8(8);

    size_t alloc_overhead = sizeof(struct mehcached_item);
#ifdef MEHCACHED_ALLOC_DYNAMIC
    alloc_overhead += MEHCAHCED_DYNAMIC_OVERHEAD;
#endif

    size_t owner_thread_id[MEHCACHED_MAX_PARTITIONS];

    bool concurrent_table_read = (concurrency_mode >= CONCURRENCY_MODE_CREW);
    bool concurrent_table_write = (concurrency_mode >= CONCURRENCY_MODE_CRCW);
    bool concurrent_alloc_write = (concurrency_mode >= CONCURRENCY_MODE_CRCWS);

    printf("initializing shm\n");
	const size_t page_size = 1048576 * 2;
	const size_t num_numa_nodes = 2;
    const size_t num_pages_to_try = 16384;
    const size_t num_pages_to_reserve = 16384 - 2048;   // give 2048 pages to dpdk

	mehcached_shm_init(page_size, num_numa_nodes, num_pages_to_try, num_pages_to_reserve);


    printf("initializing DPDK\n");
    uint64_t cpu_mask = 0;
    size_t thread_id;
	for (thread_id = 0; thread_id < num_threads; thread_id++)
		cpu_mask |= (uint64_t)1 << thread_id;
	char cpu_mask_str[10];
	snprintf(cpu_mask_str, sizeof(cpu_mask_str), "%zx", cpu_mask);

    char memory_str[10];
	snprintf(memory_str, sizeof(memory_str), "%zu", (num_pages_to_try - num_pages_to_reserve) * 2);   // * 2 is because the used huge page size is 2 MB

	char *rte_argv[] = {"", "-c", cpu_mask_str, "-m", memory_str, "-n", "4"};
	int rte_argc = sizeof(rte_argv) / sizeof(rte_argv[0]);

    rte_set_log_level(RTE_LOG_NOTICE);

	int ret = rte_eal_init(rte_argc, rte_argv);
	if (ret < 0)
		rte_panic("Cannot init EAL\n");


    printf("allocating memory\n");
    uint8_t *keys = (uint8_t *)mehcached_shm_malloc_striped(key_length * num_items * 2);
    assert(keys);
    uint64_t *key_hashes = (uint64_t *)mehcached_shm_malloc_striped(sizeof(uint64_t) * num_items * 2);
    assert(key_hashes);
    uint16_t *key_parts = (uint16_t *)mehcached_shm_malloc_striped(sizeof(uint16_t) * num_items * 2);
    assert(key_parts);
    uint8_t *values = (uint8_t *)mehcached_shm_malloc_striped(value_length * num_items * 2);
    assert(values);

    uint64_t *op_count = (uint64_t *)malloc(sizeof(uint64_t) * num_threads);
    assert(op_count);
    uint8_t **op_types = (uint8_t **)malloc(sizeof(uint8_t *) * num_threads);
    assert(op_types);
    uint8_t **op_keys = (uint8_t **)malloc(sizeof(uint8_t *) * num_threads);
    assert(op_keys);
    uint64_t **op_key_hashes = (uint64_t **)malloc(sizeof(uint64_t *) * num_threads);
    assert(op_key_hashes);
    uint16_t **op_key_parts = (uint16_t **)malloc(sizeof(uint16_t *) * num_threads);
    assert(op_key_parts);
    uint8_t **op_values = (uint8_t **)malloc(sizeof(uint8_t *) * num_threads);
    assert(op_values);

    for (thread_id = 0; thread_id < num_threads; thread_id++)
    {
        op_types[thread_id] = (uint8_t *)mehcached_shm_malloc_contiguous(num_operations, thread_id);
        assert(op_types[thread_id]);
        op_keys[thread_id] = (uint8_t *)mehcached_shm_malloc_contiguous(key_length * num_operations, thread_id);
        assert(op_keys[thread_id]);
        op_key_hashes[thread_id] = (uint64_t *)mehcached_shm_malloc_contiguous(sizeof(uint64_t) * num_operations, thread_id);
        assert(op_key_hashes[thread_id]);
        op_key_parts[thread_id] = (uint16_t *)mehcached_shm_malloc_contiguous(sizeof(uint16_t) * num_operations, thread_id);
        assert(op_key_parts[thread_id]);
        op_values[thread_id] = (uint8_t *)mehcached_shm_malloc_contiguous(value_length * num_operations, thread_id);
        assert(op_values[thread_id]);
    }

    struct mehcached_table *tables[num_partitions];

    struct proc_arg args[num_threads];

    size_t mem_diff = (size_t)-1;
    double add_ops = -1.;
    double set_ops = -1.;
    double get_hit_ops = -1.;
    double get_miss_ops = -1.;
    double get_set_95_ops = -1.;
    double get_set_50_ops = -1.;
    double delete_ops = -1.;
    double set_1_ops = -1.;
    double get_1_ops = -1.;

    size_t i;
    struct timeval tv_start;
    struct timeval tv_end;
    double diff;

    printf("generating %zu items (including %zu miss items)\n", num_items, num_items);
    for (i = 0; i < num_items * 2; i++)
    {
        *(size_t *)(keys + i * key_length) = i;
        *(key_hashes + i) = hash(keys + i * key_length, key_length);
        *(key_parts + i) = mehcached_get_partition_id(*(key_hashes + i), (uint16_t)num_partitions);
        *(size_t *)(values + i * value_length) = i;
    }
    printf("\n");

    perf_count_t pc = benchmark_perf_count_init();

    size_t mem_start = mehcached_get_memuse();

    size_t partition_id;
    for (partition_id = 0; partition_id < num_partitions; partition_id++)
    {
        owner_thread_id[partition_id] = partition_id % num_threads;

        size_t table_numa_node = rte_lcore_to_socket_id((unsigned int)owner_thread_id[partition_id]);
        // TODO: support CRCW (multiple allocs)
        size_t alloc_numa_nodes[1];
        alloc_numa_nodes[0] = rte_lcore_to_socket_id((unsigned int)owner_thread_id[partition_id]);

        tables[partition_id] = mehcached_shm_malloc_contiguous(sizeof(struct mehcached_table), owner_thread_id[partition_id]);
        assert(tables[partition_id]);

        mehcached_table_init(tables[partition_id], (num_items + MEHCACHED_ITEMS_PER_BUCKET - 1) / MEHCACHED_ITEMS_PER_BUCKET / num_partitions, 1, num_items * /*MEHCACHED_ROUNDUP64*/(alloc_overhead + key_length + value_length + (num_partitions - 1)) / 1 / num_partitions, concurrent_table_read, concurrent_table_write, concurrent_alloc_write, table_numa_node, alloc_numa_nodes, mth_threshold);
    }

    for (thread_id = 0; thread_id < num_threads; thread_id++)
    {
        args[thread_id].num_threads = num_threads;
        args[thread_id].key_length = key_length;
        args[thread_id].value_length = value_length;
        args[thread_id].op_types = op_types[thread_id];
        args[thread_id].op_keys = op_keys[thread_id];
        args[thread_id].op_key_hashes = op_key_hashes[thread_id];
        args[thread_id].op_key_parts = op_key_parts[thread_id];
        args[thread_id].op_values = op_values[thread_id];
        args[thread_id].num_partitions = num_partitions;
        for (partition_id = 0; partition_id < num_partitions; partition_id++)
		{
            args[thread_id].tables[partition_id] = tables[partition_id];
            args[thread_id].owner_thread_id[partition_id] = owner_thread_id[partition_id];
		}
    }

    benchmark_mode_t benchmark_mode;
    for (benchmark_mode = 0; benchmark_mode < BENCHMARK_MODE_MAX; benchmark_mode++)
    {
        switch (benchmark_mode)
        {
            case BENCHMARK_MODE_ADD:
                printf("adding %zu items\n", num_items);
                break;
            case BENCHMARK_MODE_SET:
                printf("setting %zu items\n", num_items);
                break;
            case BENCHMARK_MODE_GET_HIT:
                printf("getting %zu items (hit)\n", num_items);
                break;
            case BENCHMARK_MODE_GET_MISS:
                printf("getting %zu items (miss)\n", num_items);
                break;
            case BENCHMARK_MODE_GET_SET_95:
                printf("getting/setting %zu items (95%% get)\n", num_items);
                break;
            case BENCHMARK_MODE_GET_SET_50:
                printf("getting/setting %zu items (50%% get)\n", num_items);
                break;
            case BENCHMARK_MODE_DELETE:
                printf("deleting %zu items\n", num_items);
                break;
            case BENCHMARK_MODE_SET_1:
                printf("setting 1 item\n");
                break;
            case BENCHMARK_MODE_GET_1:
                printf("getting 1 item\n");
                break;
            default:
                assert(false);
        }

        printf("generating workload\n");
        uint64_t thread_rand_state = 1;
        //uint64_t key_rand_state = 2;
        uint64_t op_type_rand_state = 3;

        uint32_t get_threshold = 0;
        if (benchmark_mode == BENCHMARK_MODE_ADD || benchmark_mode == BENCHMARK_MODE_SET || benchmark_mode == BENCHMARK_MODE_DELETE || benchmark_mode == BENCHMARK_MODE_SET_1)
            get_threshold = (uint32_t)(0.0 * (double)((uint32_t)-1));
        else if (benchmark_mode == BENCHMARK_MODE_GET_HIT || benchmark_mode == BENCHMARK_MODE_GET_MISS || benchmark_mode == BENCHMARK_MODE_GET_1)
            get_threshold = (uint32_t)(1.0 * (double)((uint32_t)-1));
        else if (benchmark_mode == BENCHMARK_MODE_GET_SET_95)
            get_threshold = (uint32_t)(0.95 * (double)((uint32_t)-1));
        else if (benchmark_mode == BENCHMARK_MODE_GET_SET_50)
            get_threshold = (uint32_t)(0.5 * (double)((uint32_t)-1));
        else
            assert(false);

        struct zipf_gen_state zipf_state;
        mehcached_zipf_init(&zipf_state, num_items, zipf_theta, (uint64_t)benchmark_mode);

        memset(op_count, 0, sizeof(uint64_t) * num_threads);
        size_t j;
        for (j = 0; j < num_operations; j++)
        {
            size_t i;
            if (benchmark_mode == BENCHMARK_MODE_ADD || benchmark_mode == BENCHMARK_MODE_DELETE)
            {
                if (j >= num_items)
                    break;
                i = j;
            }
            else if (benchmark_mode == BENCHMARK_MODE_GET_1 || benchmark_mode == BENCHMARK_MODE_SET_1)
                i = 0;
            else
            {
                //i = mehcached_rand(&key_rand_state) % num_items;
                i = mehcached_zipf_next(&zipf_state);
                if (benchmark_mode == BENCHMARK_MODE_GET_MISS)
                    i += num_items;
            }

            uint16_t partition_id = key_parts[i];

            uint32_t op_r = mehcached_rand(&op_type_rand_state);
            bool is_get = op_r <= get_threshold;

            size_t thread_id;
            if (is_get)
            {
                if (concurrency_mode <= CONCURRENCY_MODE_EREW)
                    thread_id = owner_thread_id[partition_id];
                else
                    thread_id = (owner_thread_id[partition_id] % 2) + (mehcached_rand(&thread_rand_state) % (num_threads / 2)) * 2;
            }
            else
            {
                if (concurrency_mode <= CONCURRENCY_MODE_CREW)
                    thread_id = owner_thread_id[partition_id];
                else
                    thread_id = (owner_thread_id[partition_id] % 2) + (mehcached_rand(&thread_rand_state) % (num_threads / 2)) * 2;
            }

            if (op_count[thread_id] < max_num_operations_per_thread)
            {
                op_types[thread_id][op_count[thread_id]] = is_get ? 0 : 1;
                memcpy(op_keys[thread_id] + key_length * op_count[thread_id], keys + key_length * i, key_length);
                op_key_hashes[thread_id][op_count[thread_id]] = key_hashes[i];
                op_key_parts[thread_id][op_count[thread_id]] = key_parts[i];
                memcpy(op_values[thread_id] + value_length * op_count[thread_id], values + value_length * i, value_length);
                op_count[thread_id]++;
            }
            else
                break;
        }

        printf("executing workload\n");

        for (thread_id = 0; thread_id < num_threads; thread_id++)
        {
            args[thread_id].op_count = op_count[thread_id];
            args[thread_id].benchmark_mode = benchmark_mode;
            args[thread_id].junk = 0;
            args[thread_id].success_count = 0;
        }

        benchmark_perf_count_start(pc);
        gettimeofday(&tv_start, NULL);

        running_threads = 0;
        memory_barrier();

        for (thread_id = 1; thread_id < num_threads; thread_id++)
			rte_eal_remote_launch(benchmark_proc, &args[thread_id], (unsigned int)thread_id);
		benchmark_proc(&args[0]);

		rte_eal_mp_wait_lcore();

        gettimeofday(&tv_end, NULL);
        benchmark_perf_count_stop(pc);
        diff = (double)(tv_end.tv_sec - tv_start.tv_sec) * 1. + (double)(tv_end.tv_usec - tv_start.tv_usec) * 0.000001;

        size_t success_count = 0;
        size_t operations = 0;
        for (thread_id = 0; thread_id < num_threads; thread_id++)
        {
            success_count += args[thread_id].success_count;
            operations += args[thread_id].op_count;
        }

        printf("operations: %zu\n", operations);
        printf("success_count: %zu\n", success_count);

        switch (benchmark_mode)
        {
            case BENCHMARK_MODE_ADD:
                add_ops = (double)operations / diff;
                mem_diff = mehcached_get_memuse() - mem_start;
                break;
            case BENCHMARK_MODE_SET:
                set_ops = (double)operations / diff;
                break;
            case BENCHMARK_MODE_GET_HIT:
                get_hit_ops = (double)operations / diff;
                break;
            case BENCHMARK_MODE_GET_MISS:
                get_miss_ops = (double)operations / diff;
                break;
            case BENCHMARK_MODE_GET_SET_95:
                get_set_95_ops = (double)operations / diff;
                break;
            case BENCHMARK_MODE_GET_SET_50:
                get_set_50_ops = (double)operations / diff;
                break;
            case BENCHMARK_MODE_DELETE:
                delete_ops = (double)operations / diff;
                break;
            case BENCHMARK_MODE_SET_1:
                set_1_ops = (double)operations / diff;
                break;
            case BENCHMARK_MODE_GET_1:
                get_1_ops = (double)operations / diff;
                break;
            default:
                assert(false);
        }

        for (partition_id = 0; partition_id < num_partitions; partition_id++)
        {
            mehcached_print_stats(tables[partition_id]);
            mehcached_reset_table_stats(tables[partition_id]);
        }

        printf("\n");
    }

    if (args[0].junk == 1)
        printf("junk: %zu (ignore this line)\n", args[0].junk);

    // for (i = 0; i < num_threads; i++)
    //     mehcached_shm_free(args[i].existing_items);
    // mehcached_shm_free(c.keys);
    // mehcached_shm_free(c.key_hashes);
    // mehcached_shm_free(c.miss_keys);
    // mehcached_shm_free(c.miss_key_hashes);
    // mehcached_shm_free(c.values);

    // for (i = 0; i < num_tables; i++)
    //     mehcached_table_free(&tables[i]);

    benchmark_perf_count_free(pc);

    printf("memory:     %10.2lf MB\n", (double)mem_diff * 0.000001);
    printf("add:        %10.2lf Mops\n", add_ops * 0.000001);
    printf("set:        %10.2lf Mops\n", set_ops * 0.000001);
    printf("get_hit:    %10.2lf Mops\n", get_hit_ops * 0.000001);
    printf("get_miss:   %10.2lf Mops\n", get_miss_ops * 0.000001);
    printf("get_set_95: %10.2lf Mops\n", get_set_95_ops * 0.000001);
    printf("get_set_50: %10.2lf Mops\n", get_set_50_ops * 0.000001);
    printf("delete:     %10.2lf Mops\n", delete_ops * 0.000001);
    printf("set_1:      %10.2lf Mops\n", set_1_ops * 0.000001);
    printf("get_1:      %10.2lf Mops\n", get_1_ops * 0.000001);
}

int
main(int argc, const char *argv[])
{
    if (argc < 4)
    {
        printf("%s { EREW | CREW | CRCWS } ZIPF-THETA MTH-THRESHOLD\n", argv[0]);
        return EXIT_FAILURE;
    }

    if (strcmp(argv[1], "EREW") == 0)
        benchmark(CONCURRENCY_MODE_EREW, atof(argv[2]), atof(argv[3]));
    else if (strcmp(argv[1], "CREW") == 0)
        benchmark(CONCURRENCY_MODE_CREW, atof(argv[2]), atof(argv[3]));
    else if (strcmp(argv[1], "CRCWS") == 0)
        benchmark(CONCURRENCY_MODE_CRCWS, atof(argv[2]), atof(argv[3]));
    else
    {
        printf("not supported concurrency mode\n");
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}

