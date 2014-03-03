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
#include <assert.h>

#include "mehcached.h"
#include "hash.h"

void
test_load()
{
    printf("test_load()\n");

    const size_t num_items = 1048576;
    //const size_t num_items = 1048576 * 10;

    struct mehcached_table table_o;
    struct mehcached_table *table = &table_o;
    size_t numa_nodes[] = {(size_t)-1};
    size_t alloc_overhead = sizeof(struct mehcached_item);
#ifdef MEHCACHED_ALLOC_DYNAMIC
    alloc_overhead += MEHCAHCED_DYNAMIC_OVERHEAD;
#endif
    mehcached_table_init(table, (num_items + MEHCACHED_ITEMS_PER_BUCKET - 1) / MEHCACHED_ITEMS_PER_BUCKET, 1, num_items * /*MEHCACHED_ROUNDUP64*/(alloc_overhead + 8 + 8), false, false, false, numa_nodes[0], numa_nodes, MEHCACHED_MTH_THRESHOLD_FIFO);

    bool first_failure = false;
    size_t first_failure_i = 0;
    size_t success_count = 0;

    size_t i;
    for (i = 0; i < num_items; i++)
    {
        size_t key = i;
        size_t value = i;
        uint64_t key_hash = hash((const uint8_t *)&key, sizeof(key));
        mehcached_set(0, table, key_hash, (const uint8_t *)&key, sizeof(key), (const uint8_t *)&value, sizeof(value), 0, false);
    }

    for (i = 0; i < num_items; i++)
    {
        size_t key = i;
        size_t value;
        size_t value_len = sizeof(value);
        uint64_t key_hash = hash((const uint8_t *)&key, sizeof(key));

        if (mehcached_get(0, table, key_hash, (const uint8_t *)&key, sizeof(key), (uint8_t *)&value, &value_len, NULL, false))
            success_count++;
        else
        {
            if (!first_failure)
            {
                first_failure = true;
                first_failure_i = i;
            }
        }
    }

    printf("first_failure: %zu (%.2f%%)\n", first_failure_i, 100. * (double)first_failure_i / (double)num_items);
    printf("success_count: %zu (%.2f%%)\n", success_count, 100. * (double)success_count / (double)num_items);

    //mehcached_print_buckets(table);
    mehcached_print_stats(table);

    mehcached_table_free(table);
}

int
main(int argc MEHCACHED_UNUSED, const char *argv[] MEHCACHED_UNUSED)
{
	const size_t page_size = 1048576 * 2;
	const size_t num_numa_nodes = 2;
    const size_t num_pages_to_try = 16384;
    const size_t num_pages_to_reserve = 16384 - 2048;   // give 2048 pages to dpdk

	mehcached_shm_init(page_size, num_numa_nodes, num_pages_to_try, num_pages_to_reserve);

    test_load();

    return EXIT_SUCCESS;
}

