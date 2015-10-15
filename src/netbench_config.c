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

#include "netbench_config.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

struct mehcached_server_conf *
mehcached_get_server_conf(const char *filename, const char *server_name)
{
	FILE *fp = fopen(filename, "r");
	if (!fp)
	{
		fprintf(stderr, "cannot open %s\n", filename);
		return NULL;
	}

	struct mehcached_server_conf *conf = malloc(sizeof(struct mehcached_server_conf));
	memset(conf, 0, sizeof(struct mehcached_server_conf));

	while (true)
	{
		char buf[4096];
		int ret = fscanf(fp, "server,%[^,\n]\n", buf);
		if (ret == EOF)
			break;
		if (strcmp(buf, server_name) != 0)
		{
			// skip
			while (true)
			{
				if (fgets(buf, sizeof(buf), fp) == NULL)
					break;
				if (buf[0] == '\n')
					break;
			}
			continue;
		}

		while (true)
		{
			if (fgets(buf, sizeof(buf), fp) == NULL)
				break;

			{
				char ip_addr[4096];
				char mac_addr[4096];
				ret = sscanf(buf, "server_port,%[^,],%[^,\n]\n", mac_addr, ip_addr);
				if (ret == 2)
				{
					size_t i;
					char *p = mac_addr;
					for (i = 0; i < 6; i++, p++)
						conf->ports[conf->num_ports].mac_addr[i] = (uint8_t)strtoul(p, &p, 16);
					p = ip_addr;
					for (i = 0; i < 4; i++, p++)
						conf->ports[conf->num_ports].ip_addr[i] = (uint8_t)strtoul(p, &p, 10);
					conf->num_ports++;
					assert(conf->num_ports <= MEHCACHED_MAX_PORTS);
					continue;
				}
				else if (ret != 0)
				{
					fprintf(stderr, "parse error: %s (in %s)\n", buf, filename);
					continue;
				}
			}
			{
				char port_ids[4096];
				ret = sscanf(buf, "server_thread,%[^,\n]\n", port_ids);
				if (ret == 1)
				{
					if (strcmp(port_ids, "-") != 0)
					{
						char *p = port_ids;
						while (*p != 0)
						{
							conf->threads[conf->num_threads].port_ids[conf->threads[conf->num_threads].num_ports] = (uint8_t)strtoul(p, &p, 10);
							conf->threads[conf->num_threads].num_ports++;
							assert(conf->threads[conf->num_threads].num_ports <= MEHCACHED_MAX_PORTS);
							if (*p != 0) p++;
						}
					}
					conf->num_threads++;
					assert(conf->num_threads <= MEHCACHED_MAX_THREADS);
					continue;
				}
				else if (ret != 0)
				{
					fprintf(stderr, "parse error: %s (in %s)\n", buf, filename);
					continue;
				}
			}
			{
				uint64_t num_items;
				uint64_t alloc_size;
				uint8_t concurrent_table_read;
				uint8_t concurrent_table_write;
				uint8_t concurrent_alloc_write;
				uint8_t thread_id;
				double mth_threshold;
				ret = sscanf(buf, "server_partition,%lu,%lu,%hhu,%hhu,%hhu,%hhu,%lf\n", &num_items, &alloc_size, &concurrent_table_read, &concurrent_table_write, &concurrent_alloc_write, &thread_id, &mth_threshold);
				if (ret == 7)
				{
					conf->partitions[conf->num_partitions].num_items = num_items;
					conf->partitions[conf->num_partitions].alloc_size = alloc_size;
					conf->partitions[conf->num_partitions].concurrent_table_read = concurrent_table_read;
					conf->partitions[conf->num_partitions].concurrent_table_write = concurrent_table_write;
					conf->partitions[conf->num_partitions].concurrent_alloc_write = concurrent_alloc_write;
					conf->partitions[conf->num_partitions].thread_id = thread_id;
					conf->partitions[conf->num_partitions].mth_threshold = mth_threshold;
					conf->num_partitions++;
					assert(conf->num_partitions <= MEHCACHED_MAX_PARTITIONS);
					continue;
				}
				else if (ret != 0)
				{
					fprintf(stderr, "parse error: %s (in %s)\n", buf, filename);
					continue;
				}
			}
			{
				uint64_t key_hash;
				uint8_t thread_id;
				ret = sscanf(buf, "server_hot_item,%lx,%hhu\n", &key_hash, &thread_id);
				if (ret == 2)
				{
					conf->hot_items[conf->num_hot_items].key_hash = key_hash;
					conf->hot_items[conf->num_hot_items].thread_id = thread_id;
					conf->num_hot_items++;
					assert(conf->num_hot_items <= MEHCACHED_MAX_HOT_ITEMS);
					continue;
				}
				else if (ret != 0)
				{
					fprintf(stderr, "parse error: %s (in %s)\n", buf, filename);
					continue;
				}
			}
			if (buf[0] == '\n')
				break;
			fprintf(stderr, "parse error: %s (in %s)\n", buf, filename);
		}
	}

	fclose(fp);
	return conf;
}

struct mehcached_client_conf *
mehcached_get_client_conf(const char *filename, const char *client_name)
{
	FILE *fp = fopen(filename, "r");
	if (!fp)
	{
		fprintf(stderr, "cannot open %s\n", filename);
		return NULL;
	}

	struct mehcached_client_conf *conf = malloc(sizeof(struct mehcached_client_conf));
	memset(conf, 0, sizeof(struct mehcached_client_conf));

	while (true)
	{
		char buf[4096];
		int ret = fscanf(fp, "client,%[^,\n]\n", buf);
		if (ret == EOF)
			break;
		if (strcmp(buf, client_name) != 0)
		{
			// skip
			while (true)
			{
				if (fgets(buf, sizeof(buf), fp) == NULL)
					break;
				if (buf[0] == '\n')
					break;
			}
			continue;
		}

		while (true)
		{
			if (fgets(buf, sizeof(buf), fp) == NULL)
				break;

			{
				char ip_addr[4096];
				char mac_addr[4096];
				ret = sscanf(buf, "client_port,%[^,],%[^,\n]\n", mac_addr, ip_addr);
				if (ret == 2)
				{
					size_t i;
					char *p = mac_addr;
					for (i = 0; i < 6; i++, p++)
						conf->ports[conf->num_ports].mac_addr[i] = (uint8_t)strtoul(p, &p, 16);
					p = ip_addr;
					for (i = 0; i < 4; i++, p++)
						conf->ports[conf->num_ports].ip_addr[i] = (uint8_t)strtoul(p, &p, 10);
					conf->num_ports++;
					assert(conf->num_ports <= MEHCACHED_MAX_PORTS);
					continue;
				}
				else if (ret != 0)
				{
					fprintf(stderr, "parse error: %s (in %s)\n", buf, filename);
					continue;
				}
			}
			{
				if (strcmp(buf, "client_thread,\n") == 0)
				{
					conf->num_threads++;
					assert(conf->num_threads <= MEHCACHED_MAX_THREADS);
					continue;
				}
			}
			if (buf[0] == '\n')
				break;
			fprintf(stderr, "parse error: %s (in %s)\n", buf, filename);
		}
	}

	fclose(fp);
	return conf;
}

struct mehcached_prepopulation_conf *
mehcached_get_prepopulation_conf(const char *filename, const char *server_name)
{
	FILE *fp = fopen(filename, "r");
	if (!fp)
	{
		fprintf(stderr, "cannot open %s\n", filename);
		return NULL;
	}

	struct mehcached_prepopulation_conf *conf = malloc(sizeof(struct mehcached_prepopulation_conf));
	memset(conf, 0, sizeof(struct mehcached_prepopulation_conf));

	while (true)
	{
		char buf[4096];
		int ret = fscanf(fp, "prepopulation,%[^,\n]\n", buf);
		if (ret == EOF)
			break;
		if (strcmp(buf, server_name) != 0)
		{
			// skip
			while (true)
			{
				if (fgets(buf, sizeof(buf), fp) == NULL)
					break;
				if (buf[0] == '\n')
					break;
			}
			continue;
		}

		while (true)
		{
			if (fgets(buf, sizeof(buf), fp) == NULL)
				break;

			{
				uint64_t num_items;
				size_t key_length;
				size_t value_length;
				int ret = sscanf(buf, "dataset,%lu,%zu,%zu\n", &num_items, &key_length, &value_length);
				if (ret == 3)
				{
					conf->num_items = num_items;
					conf->key_length = key_length;
					conf->value_length = value_length;
					continue;
				}
				else if (ret != 0)
				{
					fprintf(stderr, "parse error: %s (in %s)\n", buf, filename);
					continue;
				}
			}

			if (buf[0] == '\n')
				break;
			fprintf(stderr, "parse error: %s (in %s)\n", buf, filename);
		}
	}

	fclose(fp);
	return conf;
}
struct mehcached_workload_conf *
mehcached_get_workload_conf(const char *filename, const char *client_name)
{
	FILE *fp = fopen(filename, "r");
	if (!fp)
	{
		fprintf(stderr, "cannot open %s\n", filename);
		return NULL;
	}

	struct mehcached_workload_conf *conf = malloc(sizeof(struct mehcached_workload_conf));
	memset(conf, 0, sizeof(struct mehcached_workload_conf));

	while (true)
	{
		char buf[4096];
		int ret = fscanf(fp, "workload,%[^,\n]\n", buf);
		if (ret == EOF)
			break;
		if (strcmp(buf, client_name) != 0)
		{
			// skip
			while (true)
			{
				if (fgets(buf, sizeof(buf), fp) == NULL)
					break;
				if (buf[0] == '\n')
					break;
			}
			continue;
		}

		while (true)
		{
			if (fgets(buf, sizeof(buf), fp) == NULL)
				break;

			{
				char port_ids[4096];
				char server_name[4096];
				int8_t partition_mode;
				uint64_t num_items;
				size_t key_length;
				size_t value_length;
				double zipf_theta;
				double get_ratio;
				double put_ratio;
				double increment_ratio;
				uint8_t batch_size;
				uint64_t num_operations;
				double duration;
				int ret = sscanf(buf, "workload_thread,%[^,],%[^,],%hhd,%lu,%zu,%zu,%lf,%lf,%lf,%lf,%hhu,%lu,%lf\n", port_ids, server_name, &partition_mode, &num_items, &key_length, &value_length, &zipf_theta, &get_ratio, &put_ratio, &increment_ratio, &batch_size, &num_operations, &duration);
				if (ret == 13)
				{
					if (strcmp(port_ids, "-") != 0)
					{
						char *p = port_ids;
						while (*p != 0)
						{
							conf->threads[conf->num_threads].port_ids[conf->threads[conf->num_threads].num_ports] = (uint8_t)strtoul(p, &p, 10);
							conf->threads[conf->num_threads].num_ports++;
							assert(conf->threads[conf->num_threads].num_ports <= MEHCACHED_MAX_PORTS);
							if (*p != 0) p++;
						}
					}
					strcpy(conf->threads[conf->num_threads].server_name, server_name);
					conf->threads[conf->num_threads].partition_mode = partition_mode;
					conf->threads[conf->num_threads].num_items = num_items;
					conf->threads[conf->num_threads].key_length = key_length;
					conf->threads[conf->num_threads].value_length = value_length;
					conf->threads[conf->num_threads].zipf_theta = zipf_theta;
					conf->threads[conf->num_threads].get_ratio = get_ratio;
					conf->threads[conf->num_threads].put_ratio = put_ratio;
					conf->threads[conf->num_threads].increment_ratio = increment_ratio;
					conf->threads[conf->num_threads].batch_size = batch_size;
					conf->threads[conf->num_threads].num_operations = num_operations;
					conf->threads[conf->num_threads].duration = duration;
					conf->num_threads++;
					assert(conf->num_threads <= MEHCACHED_MAX_THREADS);
					continue;
				}
				else if (ret != 0)
				{
					fprintf(stderr, "parse error: %s (in %s)\n", buf, filename);
					continue;
				}
			}

			if (buf[0] == '\n')
				break;
			fprintf(stderr, "parse error: %s (in %s)\n", buf, filename);
		}
	}

	fclose(fp);
	return conf;
}
