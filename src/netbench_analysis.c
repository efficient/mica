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
#include <signal.h>

#include "mehcached.h"
#include "hash.h"
#include "zipf.h"
#include "stopwatch.h"
#include "netbench_config.h"

// uncomment this to use CREW instead of EREW
//#define MEHCACHED_LOAD_BALANCE_USE_CREW_PARTITIONS

static
uint64_t
mehcached_hash_key(uint64_t int_key)
{
	return hash((const uint8_t *)&int_key, 8);
}

static
void
mehcached_print_array_uint64_t(uint64_t arr[], size_t num_elements)
{
	size_t i;
	for (i = 0; i < num_elements; i++)
		printf("[%3zu]%lu ", i, arr[i]);
	printf("\n");
}

static
void
mehcached_print_array_normalized(uint64_t arr[], size_t num_elements)
{
	size_t i;
	uint64_t max_elem = 0;
	uint64_t min_elem = (uint64_t)-1;
	uint64_t sum_elem = 0;
	for (i = 0; i < num_elements; i++)
	{
		if (max_elem < arr[i])
			max_elem = arr[i];
		if (min_elem > arr[i])
			min_elem = arr[i];
		sum_elem += arr[i];
	}
	if (max_elem == 0)
		max_elem = 1;	// to avoid divide by zero
	for (i = 0; i < num_elements; i++)
	{
		printf("[%3zu]%lf", i, (double)arr[i] / (double)max_elem);
		if (i % 8 != 7) 
			printf(" ");
	        else if (i != num_elements - 1)
			printf("\n"); 
	}
	/*
	printf("\n");
	for (i = 0; i < num_elements; i++) //single line, seperate by blank space, for easy post processing for load CDF
	{
		printf("%lf", (double)arr[i] / (double)max_elem);
		// if (i % 8 != 7) 
			printf(" ");
		//else if (i != num_elements - 1) 
		//	printf("\n");
	}
	*/
	printf("\n");
	printf("min = %lf\n", (double)min_elem / (double)max_elem);
	printf("avg = %lf\n", (double)sum_elem / (double)max_elem / (double)num_elements);
}

static
void
mehcached_calc_thread_load(uint64_t out_thread_load[], const uint64_t partition_load[], const uint64_t hot_item_load[], const uint8_t partition_to_thread_org[], const uint8_t hot_item_to_thread_org[], const uint8_t partition_to_thread_new[], const uint8_t hot_item_to_thread_new[], uint8_t num_threads, uint64_t num_partitions, uint64_t num_hot_items, uint8_t num_numa_nodes, double get_ratio, bool isolated_server_numa_nodes)
{
	uint16_t partition_id;
	uint8_t thread_id;
	uint64_t key;

#ifndef MEHCACHED_LOAD_BALANCE_USE_CREW_PARTITIONS
	(void)partition_to_thread_org;
#endif

	for (thread_id = 0; thread_id < num_threads; thread_id++)
		out_thread_load[thread_id] = 0;

	for (partition_id = 0; partition_id < num_partitions; partition_id++)
	{
#ifndef MEHCACHED_LOAD_BALANCE_USE_CREW_PARTITIONS
		// EREW
		thread_id = partition_to_thread_new[partition_id];
		out_thread_load[thread_id] += partition_load[partition_id];
#else
		// CR
		uint8_t numa_node_id = (uint8_t)(partition_to_thread_org[partition_id] & 1);
		for (thread_id = 0; thread_id < num_threads; thread_id++)
		{
			if (isolated_server_numa_nodes)
			{
				if (thread_id % num_numa_nodes != numa_node_id)
					continue;
				out_thread_load[thread_id] += (uint64_t)((double)partition_load[partition_id] * get_ratio / (double)(num_threads / num_numa_nodes));
			}
			else
				out_thread_load[thread_id] += (uint64_t)((double)partition_load[partition_id] * get_ratio / (double)num_threads);
		}
		// EW
		thread_id = partition_to_thread_new[partition_id];
		out_thread_load[thread_id] += (uint64_t)((double)partition_load[partition_id] * (1. - get_ratio));
#endif
	}

	for (key = 0; key < num_hot_items; key++)
	{
		// CR
		uint8_t numa_node_id = (uint8_t)(hot_item_to_thread_org[key] & 1);
		for (thread_id = 0; thread_id < num_threads; thread_id++)
		{
			if (isolated_server_numa_nodes)
			{
				if (thread_id % num_numa_nodes != numa_node_id)
					continue;
				out_thread_load[thread_id] += (uint64_t)((double)hot_item_load[key] * get_ratio / (double)(num_threads / num_numa_nodes));
			}
			else
				out_thread_load[thread_id] += (uint64_t)((double)hot_item_load[key] * get_ratio / (double)num_threads);
		}
		// EW
		thread_id = hot_item_to_thread_new[key];
		out_thread_load[thread_id] += (uint64_t)((double)hot_item_load[key] * (1. - get_ratio));
	}
}

static
void
mehcached_load_balance(const uint64_t partition_load[], const uint64_t hot_item_load[], const uint8_t partition_to_thread_org[], uint8_t out_partition_to_thread_new[], const uint8_t hot_item_to_thread_org[], uint8_t out_hot_item_to_thread_new[], uint8_t num_threads, uint64_t num_partitions, uint64_t num_hot_items, uint8_t num_numa_nodes, double get_ratio, bool isolated_server_numa_nodes)
{
	uint64_t max_num_entries = num_partitions + num_hot_items;
	size_t entry_type[max_num_entries];
	size_t entry_id[max_num_entries];
	uint64_t entry_load[max_num_entries];

	uint8_t numa_node_id;
	for (numa_node_id = 0; numa_node_id < num_numa_nodes; numa_node_id++)
	{
		uint16_t partition_id;
		uint64_t key;

		// enumerate all entries to consider
		uint64_t num_entries = 0;
		for (partition_id = 0; partition_id < num_partitions; partition_id++)
		{
			if (partition_to_thread_org[partition_id] % num_numa_nodes != numa_node_id)
				continue;
			entry_type[num_entries] = 0;
			entry_id[num_entries] = partition_id;
			entry_load[num_entries] = partition_load[partition_id];
			num_entries++;
		}

		for (key = 0; key < num_hot_items; key++)
		{
			if (hot_item_to_thread_org[key] % num_numa_nodes != numa_node_id)
				continue;
			entry_type[num_entries] = 1;
			entry_id[num_entries] = key;
			entry_load[num_entries] = hot_item_load[key];
			num_entries++;
		}


		uint64_t thread_load[num_threads];

		uint8_t thread_id;
		for (thread_id = 0; thread_id < num_threads; thread_id++)
			thread_load[thread_id] = 0;

		uint64_t i;
		uint64_t j;

#ifdef MEHCACHED_LOAD_BALANCE_USE_CREW_PARTITIONS
		// apply concurrent read load from partitions (CREW)
		for (i = 0; i < num_entries; i++)
			if (entry_type[i] == 0)
			{
				partition_id = (uint16_t)entry_id[i];
				for (thread_id = 0; thread_id < num_threads; thread_id++)
				{
					if (isolated_server_numa_nodes)
					{
						if (thread_id % num_numa_nodes != numa_node_id)
							continue;
						else
							thread_load[thread_id] += (uint64_t)((double)entry_load[i] * get_ratio / (double)(num_threads / num_numa_nodes));
					}
					else
						thread_load[thread_id] += (uint64_t)((double)entry_load[i] * get_ratio / (double)num_threads);
				}
				entry_load[i] = (uint64_t)((double)entry_load[i] * (1. - get_ratio));
			}
#endif

		// apply concurrent read load from hot items (CREW)
		for (i = 0; i < num_entries; i++)
			if (entry_type[i] == 1)
			{
				key = entry_id[i];
				for (thread_id = 0; thread_id < num_threads; thread_id++)
				{
					if (isolated_server_numa_nodes)
					{
						if (thread_id % num_numa_nodes != numa_node_id)
							continue;
						else
							thread_load[thread_id] += (uint64_t)((double)entry_load[i] * get_ratio / (double)(num_threads / num_numa_nodes));
					}
					else
						thread_load[thread_id] += (uint64_t)((double)entry_load[i] * get_ratio / (double)num_threads);
				}
				entry_load[i] = (uint64_t)((double)entry_load[i] * (1. - get_ratio));
			}

		// guarantee non-zero load to spread partitions across cores even when all access can be done by any core
		for (i = 0; i < num_entries; i++)
			if (entry_load[i] == 0)
				entry_load[i] = 1;

		// sort in descending order
		for (i = 0; i < num_entries; i++)
			for (j = i + 1; j < num_entries; j++)
			{
				if (entry_load[i] < entry_load[j])
				{
					size_t t0 = entry_type[i];
					entry_type[i] = entry_type[j];
					entry_type[j] = t0;
					t0 = entry_id[i];
					entry_id[i] = entry_id[j];
					entry_id[j] = t0;
					uint64_t t1 = entry_load[i];
					entry_load[i] = entry_load[j];
					entry_load[j] = t1;
				}
			}

		// best fit
		for (i = 0; i < num_entries; i++)
		{
			uint8_t min_load_thread_id = numa_node_id;
			for (thread_id = 0; thread_id < num_threads; thread_id++)
			{
				if (thread_id % num_numa_nodes != numa_node_id)	// do not move anything across NUMA nodes
					continue;
				if (thread_load[min_load_thread_id] > thread_load[thread_id])
					min_load_thread_id = thread_id;
			}

			if (entry_type[i] == 0)
			{
				//min_load_thread_id = out_partition_to_thread[entry_id[i]];	// uncomment this when we are doing object remapping only
				out_partition_to_thread_new[entry_id[i]] = min_load_thread_id;
			}
			else
				out_hot_item_to_thread_new[entry_id[i]] = min_load_thread_id;
			thread_load[min_load_thread_id] += entry_load[i];
		}
	}
}

static
void
mehcached_benchmark_analysis(uint64_t num_hot_items, double zipf_theta, double get_ratio, bool isolated_server_numa_nodes)
{
	printf("num_hot_items = %lu\n", num_hot_items);
	printf("zipf_theta = %lf\n", zipf_theta);
	printf("get_ratio = %lf\n", get_ratio);
	printf("\n");

	uint8_t num_numa_nodes = 2;
	uint8_t num_threads = 24;
	//uint16_t num_partitions = 64;
	uint16_t num_partitions = 24;
	uint64_t num_items = 192 * 1048576;

	uint64_t partition_load[num_partitions];
	uint64_t hot_item_load[num_hot_items];
	memset(partition_load, 0, sizeof(partition_load));
	memset(hot_item_load, 0, sizeof(hot_item_load));

	uint8_t partition_to_thread_org[num_partitions];
	uint8_t partition_to_thread_new[num_partitions];
	uint8_t hot_item_to_thread_org[num_hot_items];
	uint8_t hot_item_to_thread_new[num_hot_items];

	uint16_t partition_id;
	uint64_t key;
	uint64_t key_hash;

	uint64_t i;
	uint64_t num_samples = 1048576;

	// measure load
	struct zipf_gen_state zipf_state;
	mehcached_zipf_init(&zipf_state, num_items, zipf_theta, 0);

	for (i = 0; i < num_samples; i++)
	{
		key = mehcached_zipf_next(&zipf_state);

		key_hash = mehcached_hash_key(key);
	    //partition_id = (uint16_t)(key_hash >> 48) & (uint16_t)(num_partitions - 1);
		// for non power-of-two num_partitions
	    partition_id = (uint16_t)(key_hash >> 48) % (uint16_t)num_partitions;

	    if (key < num_hot_items)
		    hot_item_load[key]++;
		else
		    partition_load[partition_id]++;
	}

	// fix zero load (e.g., for single key workloads)
	for (partition_id = 0; partition_id < num_partitions; partition_id++)
		if (partition_load[partition_id] == 0)
			partition_load[partition_id] = 1;
	for (key = 0; key < num_hot_items; key++)
		if (hot_item_load[key] == 0)
			hot_item_load[key] = 1;

	// initial mapping
	for (partition_id = 0; partition_id < num_partitions; partition_id++)
	{
		partition_to_thread_org[partition_id] = (uint8_t)(partition_id % num_threads);
		partition_to_thread_new[partition_id] = partition_to_thread_org[partition_id];
	}

	for (key = 0; key < num_hot_items; key++)
	{
		key_hash = mehcached_hash_key(key);
	    //partition_id = (uint16_t)(key_hash >> 48) & (uint16_t)(num_partitions - 1);
		// for non power-of-two num_partitions
	    partition_id = (uint16_t)(key_hash >> 48) % (uint16_t)num_partitions;
	    hot_item_to_thread_org[key] = partition_to_thread_org[partition_id];
	    hot_item_to_thread_new[key] = hot_item_to_thread_org[key];
	}

	uint64_t thread_load[num_threads];

	// printf("partition load\n");
	// mehcached_print_array_normalized(partition_load, num_partitions);
	// printf("\n");
	// printf("hot item load\n");
	// mehcached_print_array_normalized(hot_item_load, num_hot_items);
	// printf("\n");

	printf("original thread load\n");
	mehcached_calc_thread_load(thread_load, partition_load, hot_item_load, partition_to_thread_org, hot_item_to_thread_org, partition_to_thread_new, hot_item_to_thread_new, num_threads, num_partitions, num_hot_items, num_numa_nodes, get_ratio, isolated_server_numa_nodes);
	mehcached_print_array_normalized(thread_load, num_threads);
	printf("\n");

	printf("load-balanced thread load\n");
	mehcached_load_balance(partition_load, hot_item_load, partition_to_thread_org, partition_to_thread_new, hot_item_to_thread_org, hot_item_to_thread_new, num_threads, num_partitions, num_hot_items, num_numa_nodes, get_ratio, isolated_server_numa_nodes);
	mehcached_calc_thread_load(thread_load, partition_load, hot_item_load, partition_to_thread_org, hot_item_to_thread_org, partition_to_thread_new, hot_item_to_thread_new, num_threads, num_partitions, num_hot_items, num_numa_nodes, get_ratio, isolated_server_numa_nodes);
	mehcached_print_array_normalized(thread_load, num_threads);
	printf("\n");

	printf("partition_to_thread: \n");
	for (partition_id = 0; partition_id < num_partitions; partition_id++)
	{
		printf("%hhu", partition_to_thread_new[partition_id]);
		if (partition_id != num_partitions - 1)
			printf(",");
	}
	printf("\n\n");

	printf("hot_item_to_thread: \n");
	for (key = 0; key < num_hot_items; key++)
	{
		key_hash = mehcached_hash_key(key);
		printf("(0x%016lx,%hhu)", key_hash, hot_item_to_thread_new[key]);
		if (key != num_hot_items - 1)
			printf(",");
	}
	printf("\n\n");
}

int
main(int argc, const char *argv[])
{
	if (argc < 5)
	{
		printf("%s NUM-HOT-ITEMS ZIPF-THETA GET-RATIO ISOLATED-SERVER-NUMA-NODES\n", argv[0]);

		mehcached_test_zipf(0.);
		mehcached_test_zipf(0.01);
		mehcached_test_zipf(0.1);
		mehcached_test_zipf(0.5);
		mehcached_test_zipf(0.9);
		mehcached_test_zipf(0.99);
		mehcached_test_zipf(0.992);
		mehcached_test_zipf(0.993);
		mehcached_test_zipf(0.994);
		mehcached_test_zipf(0.999);
		mehcached_test_zipf(1.);
		mehcached_test_zipf(10.);
		mehcached_test_zipf(20.);
		mehcached_test_zipf(30.);
		mehcached_test_zipf(40.);
		mehcached_test_zipf(50.);
		mehcached_test_zipf(100.);

		return EXIT_FAILURE;
	}

	mehcached_benchmark_analysis((uint64_t)atol(argv[1]), atof(argv[2]), atof(argv[3]), atoi(argv[4]));

    return EXIT_SUCCESS;
}
