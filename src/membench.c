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

// #ifdef NDEBUG
// #undef NDEBUG
// #endif

enum PERF_COUNT_TYPE pct[18];
size_t pct_size = sizeof(pct) / sizeof(pct[0]);

perf_count_t
benchmark_perf_count_init()
{
#ifdef USE_PERF_COUNT
	char *perf_count_type_names[] = {
		"CPUCycles",
		"Instructions",
		"CacheReferences",
		"CacheMisses",
		"L1DReadAccess",
		"L1DReadMiss",
		"L1DWriteAccess",
		"L1DWriteMiss",
		"L1DPrefetchAccess",
		"L1DPrefetchMiss",
		"LLReadAccess",
		"LLReadMiss",
		"LLWriteAccess",
		"LLWriteMiss",
		"LLPrefetchAccess",
		"LLPrefetchMiss",
		"CPUClock",
		"TaskClock",
	};

    assert(pct_size == 18);
	size_t i;
	for (i = 0; i < 18; i++)
		pct[i] = perf_count_type_by_name(perf_count_type_names[i]);

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
    BENCHMARK_MODE_RAND_WITHOUT_PREFETCH = 0,
    BENCHMARK_MODE_RAND_WITH_PREFETCH,
    BENCHMARK_MODE_MRAND_WITHOUT_PREFETCH,
    BENCHMARK_MODE_MRAND_WITH_PREFETCH,
    BENCHMARK_MODE_MAX,
} benchmark_mode_t;

struct proc_arg
{
    benchmark_mode_t benchmark_mode;
    size_t num_threads;
	size_t batch_size;

	uint64_t *array;
	size_t array_size;

	uint64_t operations;

	uint64_t junk;
};

int
benchmark_proc(void *arg)
{
    struct proc_arg *p_arg = (struct proc_arg *)arg;

    uint8_t thread_id = (uint8_t)rte_lcore_id();
    const size_t num_threads = p_arg->num_threads;
    const benchmark_mode_t benchmark_mode = p_arg->benchmark_mode;
	const size_t batch_size = p_arg->batch_size;
	const uint64_t *array = p_arg->array;
	const size_t array_size = p_arg->array_size;
	const uint64_t operations = p_arg->operations;

    int64_t i;
	size_t batch;

	uint64_t locations[batch_size];
	for (batch = 0; batch < batch_size; batch++)
		locations[batch] = batch;

    __sync_add_and_fetch((volatile size_t *)&running_threads, 1);
    while (*(volatile size_t *)&running_threads < num_threads)
        ;

	unsigned int rand_state;

	uint64_t mrand_state;

    switch (benchmark_mode)
    {
        case BENCHMARK_MODE_RAND_WITHOUT_PREFETCH:
			rand_state = (unsigned int)thread_id;
            for (i = 0; i < (int64_t)operations; i += (int64_t)batch_size)
			{
				for (batch = 0; batch < batch_size; batch++)
				{
					uint64_t r = ((uint64_t)rand_r(&rand_state) << 32) | (uint64_t)rand_r(&rand_state);
					locations[batch] = (array[locations[batch]] ^ r) & (array_size - 1);
				}
			}
			break;
        case BENCHMARK_MODE_RAND_WITH_PREFETCH:
			rand_state = (unsigned int)thread_id;
            for (i = 0; i < (int64_t)operations; i += (int64_t)batch_size)
			{
				for (batch = 0; batch < batch_size; batch++)
					__builtin_prefetch(&array[locations[batch]], 0, 0);

				for (batch = 0; batch < batch_size; batch++)
				{
					uint64_t r = ((uint64_t)rand_r(&rand_state) << 32) | (uint64_t)rand_r(&rand_state);
					locations[batch] = (array[locations[batch]] ^ r) & (array_size - 1);
				}
			}
			break;
        case BENCHMARK_MODE_MRAND_WITHOUT_PREFETCH:
			mrand_state = (uint64_t)thread_id;
            for (i = 0; i < (int64_t)operations; i += (int64_t)batch_size)
			{
				for (batch = 0; batch < batch_size; batch++)
				{
					uint64_t r = ((uint64_t)mehcached_rand(&mrand_state) << 32) | (uint64_t)mehcached_rand(&mrand_state);
					locations[batch] = (array[locations[batch]] ^ r) & (array_size - 1);
				}
			}
			break;
        case BENCHMARK_MODE_MRAND_WITH_PREFETCH:
			mrand_state = (uint64_t)thread_id;
            for (i = 0; i < (int64_t)operations; i += (int64_t)batch_size)
			{
				for (batch = 0; batch < batch_size; batch++)
					__builtin_prefetch(&array[locations[batch]], 0, 0);

				for (batch = 0; batch < batch_size; batch++)
				{
					uint64_t r = ((uint64_t)mehcached_rand(&mrand_state) << 32) | (uint64_t)mehcached_rand(&mrand_state);
					locations[batch] = (array[locations[batch]] ^ r) & (array_size - 1);
				}
			}
			break;
		default:
			assert(false);
			break;
    }

    p_arg->junk = locations[0];

	return 0;
}

void
benchmark(benchmark_mode_t benchmark_mode, size_t num_threads, size_t batch_size)
{
    printf("benchmark\n");

	if (benchmark_mode >= BENCHMARK_MODE_MAX) {
		printf("invalid benchmark mode\n");
		return;
	}

    switch (benchmark_mode)
    {
        case BENCHMARK_MODE_RAND_WITHOUT_PREFETCH:
			printf("MODE: 0: rand() jumps, without prefetch\n");
            break;
        case BENCHMARK_MODE_RAND_WITH_PREFETCH:
			printf("MODE: 1: rand() jumps, with prefetch\n");
            break;
        case BENCHMARK_MODE_MRAND_WITHOUT_PREFETCH:
			printf("MODE: 2: mehcached_rand() jumps, without prefetch\n");
			break;
        case BENCHMARK_MODE_MRAND_WITH_PREFETCH:
			printf("MODE: 3: mehcached_rand() jumps, with prefetch\n");
			break;
        default:
            assert(false);
    }

	printf("num_threads = %zu\n", num_threads);
	printf("batch_size = %zu\n", batch_size);

	// per-thread array size (must be a power of two)
    const size_t array_size = 16 * 1048576;
	printf("per_thread_array_size = %zu bytes\n", sizeof(uint64_t) * array_size);

	// per-thread operation count
	const uint64_t operations = 512 * 1048576;
	printf("per_thread_operations = %zu\n", operations);


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

    size_t mem_start = mehcached_get_memuse();

    struct proc_arg args[num_threads];
	memset(args, 0, sizeof(args));

	for (thread_id = 0; thread_id < num_threads; thread_id++)
	{
		args[thread_id].benchmark_mode = benchmark_mode;
		args[thread_id].num_threads = num_threads;
		args[thread_id].batch_size = batch_size;
		args[thread_id].array = (uint64_t *)mehcached_shm_malloc_contiguous(sizeof(uint64_t) * array_size, thread_id);
		args[thread_id].array_size = array_size;
		args[thread_id].operations = operations;
	}

    printf("initializing array\n");
	uint64_t mrand_state = 1;
	for (thread_id = 0; thread_id < num_threads; thread_id++)
	{
		uint64_t i;
		for (i = 0; i < (uint64_t)array_size; i++)
			args[thread_id].array[i] = ((uint64_t)mehcached_rand(&mrand_state) << 32) | (uint64_t)mehcached_rand(&mrand_state);
	}

    struct timeval tv_start;
    struct timeval tv_end;
    double diff;

	size_t mem_diff = mehcached_get_memuse() - mem_start;
    printf("memory: %.2lf MB\n", (double)mem_diff * 0.000001);


	printf("launching perf\n");

    perf_count_t pc = benchmark_perf_count_init();

	char cmd[1024];
	const char *perf_events = "-e cpu-cycles -e instructions -e cache-references -e cache-misses -e L1-dcache-loads -e L1-dcache-loads-misses -e L1-dcache-prefetches -e L1-dcache-prefetches-misses -e LLC-loads -e LLC-loads-misses -e LLC-prefetches -e LLC-prefetches-misses";
	snprintf(cmd, sizeof(cmd), "sleep 2; perf stat -a -C 0-%zu --log-fd 1 %s sleep 1 &", num_threads - 1, perf_events);
	ret = system(cmd);
	(void)ret;

	benchmark_perf_count_start(pc);
	gettimeofday(&tv_start, NULL);


	printf("launching benchmark\n");

	running_threads = 0;
	memory_barrier();

	for (thread_id = 1; thread_id < num_threads; thread_id++)
		rte_eal_remote_launch(benchmark_proc, &args[thread_id], (unsigned int)thread_id);
	benchmark_proc(&args[0]);

	rte_eal_mp_wait_lcore();

	gettimeofday(&tv_end, NULL);
	benchmark_perf_count_stop(pc);
	diff = (double)(tv_end.tv_sec - tv_start.tv_sec) * 1. + (double)(tv_end.tv_usec - tv_start.tv_usec) * 0.000001;


    if (args[0].junk == 1)
        printf("junk: %zu (ignore this line)\n", args[0].junk);

	printf("time: %.2lf seconds\n", diff);
	printf("tput: %.2lf Mops\n", (double)(operations * num_threads) / diff / 1000000.);

    benchmark_perf_count_free(pc);
}

int
main(int argc, const char *argv[])
{
    if (argc < 4)
    {
        printf("%s MODE NUM-THREADS BATCH-SIZE\n", argv[0]);
		printf("MODE: 0: rand() jumps, without prefetch\n");
		printf("MODE: 1: rand() jumps, with prefetch\n");
		printf("MODE: 2: mehcached_rand() jumps, without prefetch\n");
		printf("MODE: 3: mehcached_rand() jumps, with prefetch\n");
        return EXIT_FAILURE;
    }

	benchmark((benchmark_mode_t)atoi(argv[1]), (size_t)atoi(argv[2]), (size_t)atoi(argv[3]));

    return EXIT_SUCCESS;
}

