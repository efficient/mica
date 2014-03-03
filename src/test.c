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
test_basic()
{
    printf("test_basic()\n");

    struct mehcached_table table_o;
    struct mehcached_table *table = &table_o;
    size_t numa_nodes[] = {(size_t)-1};
    mehcached_table_init(table, 1, 1, 256, false, false, false, numa_nodes[0], numa_nodes, MEHCACHED_MTH_THRESHOLD_FIFO);
    assert(table);

    size_t i;
    for (i = 0; i < MEHCACHED_ITEMS_PER_BUCKET; i++)
    {
        size_t key = i;
        size_t value = i;
        uint64_t key_hash = hash((const uint8_t *)&key, sizeof(key));
        //printf("add key = %zu, value = %zu, key_hash = %lx\n", key, value, key_hash);

        if (!mehcached_set(0, table, key_hash, (const uint8_t *)&key, sizeof(key), (const uint8_t *)&value, sizeof(value), 0, false))
            assert(false);
    }
    for (i = 0; i < MEHCACHED_ITEMS_PER_BUCKET; i++)
    {
        size_t key = i;
        size_t value = 100 + i;
        uint64_t key_hash = hash((const uint8_t *)&key, sizeof(key));
        //printf("set key = %zu, value = %zu, key_hash = %lx\n", key, value, key_hash);

        if (!mehcached_set(0, table, key_hash, (const uint8_t *)&key, sizeof(key), (const uint8_t *)&value, sizeof(value), 0, true))
            assert(false);
    }

    size_t value = 0;
    for (i = 0; i < MEHCACHED_ITEMS_PER_BUCKET; i++)
    {
        size_t key = i;
        uint64_t key_hash = hash((const uint8_t *)&key, sizeof(key));

        size_t value_length = sizeof(value);
        if (!mehcached_get(0, table, key_hash, (const uint8_t *)&key, sizeof(key), (uint8_t *)&value, &value_length, NULL, false))
        {
            printf("get key = %zu, value = <not found>\n", key);
            continue;
        }
        assert(value_length == sizeof(value));
        printf("get key = %zu, value = %zu\n", key, value);
    }

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

    test_basic();

    return EXIT_SUCCESS;
}

