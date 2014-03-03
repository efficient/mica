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

#pragma once

#include "common.h"
#include "net_common.h"

//#define MEHCACHED_MAX_PORTS (8)
#define MEHCACHED_MAX_THREADS (16)
#define MEHCACHED_MAX_PARTITIONS (64)
#define MEHCACHED_MAX_WORKLOAD_THREADS (16)
#define MEHCACHED_MAX_HOT_ITEMS (64)


// common
struct mehcached_port_conf
{
	uint8_t mac_addr[6];
	uint8_t ip_addr[4];
};


// server
struct mehcached_server_thread_conf
{
	uint8_t num_ports;
	uint8_t port_ids[MEHCACHED_MAX_PORTS];
};

struct mehcached_server_partition_conf
{
	uint64_t num_items;
	uint64_t alloc_size;
	uint8_t concurrent_table_read;
	uint8_t concurrent_table_write;
	uint8_t concurrent_alloc_write;
	uint8_t thread_id;
	double mth_threshold;
};

struct mehcached_server_hot_item_conf
{
	uint64_t key_hash;
	uint8_t thread_id;
};

struct mehcached_server_conf
{
	uint8_t num_ports;
	struct mehcached_port_conf ports[MEHCACHED_MAX_PORTS];
	uint8_t num_threads;
	struct mehcached_server_thread_conf threads[MEHCACHED_MAX_THREADS];
	uint16_t num_partitions;
	struct mehcached_server_partition_conf partitions[MEHCACHED_MAX_PARTITIONS];
	uint8_t num_hot_items;
	struct mehcached_server_hot_item_conf hot_items[MEHCACHED_MAX_HOT_ITEMS];
};

#define MEHCACHED_CONCURRENT_TABLE_READ(server_conf, partition_id) ((server_conf)->partitions[partition_id].concurrent_table_read)
#define MEHCACHED_CONCURRENT_TABLE_WRITE(server_conf, partition_id) ((server_conf)->partitions[partition_id].concurrent_table_write)
#define MEHCACHED_CONCURRENT_ALLOC_WRITE(server_conf, partition_id) ((server_conf)->partitions[partition_id].concurrent_alloc_write)


// client
struct mehcached_client_conf
{
	uint8_t num_ports;
	struct mehcached_port_conf ports[MEHCACHED_MAX_PORTS];
	uint8_t num_threads;
};


// prepopulation
struct mehcached_prepopulation_conf
{
	// TODO: support multiple datasets
	uint64_t num_items;
	size_t key_length;
	size_t value_length;
};


// workload
struct mehcached_workload_thread_conf
{
	uint8_t num_ports;
	uint8_t port_ids[MEHCACHED_MAX_PORTS];
	char server_name[64];
	int8_t partition_mode;
	uint64_t num_items;
	size_t key_length;
	size_t value_length;
	double zipf_theta;
	uint8_t batch_size;
	double get_ratio;
	double put_ratio;
	double increment_ratio;
	uint64_t num_operations;
	double duration;
};

struct mehcached_workload_conf
{
	uint8_t num_threads;
	struct mehcached_workload_thread_conf threads[MEHCACHED_MAX_WORKLOAD_THREADS];
};


// functions
struct mehcached_server_conf *
mehcached_get_server_conf(const char *filename, const char *server_name);

struct mehcached_client_conf *
mehcached_get_client_conf(const char *filename, const char *client_name);

struct mehcached_prepopulation_conf *
mehcached_get_prepopulation_conf(const char *filename, const char *server_name);

struct mehcached_workload_conf *
mehcached_get_workload_conf(const char *filename, const char *client_name);
