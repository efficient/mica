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

#include <rte_mbuf.h>

#define MEHCACHED_MAX_LCORES (24)
#define MEHCACHED_MAX_NUMA_NODES (2)

#define MEHCACHED_MAX_PORTS (16)
#define MEHCACHED_MAX_QUEUES (24)

struct rte_mbuf *
mehcached_packet_alloc();

void
mehcached_packet_free(struct rte_mbuf *mbuf);

struct rte_mbuf *
mehcached_receive_packet(uint8_t port_id);

void
mehcached_receive_packets(uint8_t port_id, struct rte_mbuf **mbufs, size_t *in_out_num_mbufs);

void
mehcached_send_packet(uint8_t port_id, struct rte_mbuf *mbuf);

void
mehcached_send_packet_flush(uint8_t port_id);

void
mehcached_get_stats(uint8_t port_id, uint64_t *out_num_rx_burst, uint64_t *out_num_rx_received, uint64_t *out_num_tx_burst, uint64_t *out_num_tx_sent, uint64_t *out_num_tx_dropped);

void
mehcached_get_stats_lcore(uint8_t port_id, uint32_t lcore, uint64_t *out_num_rx_burst, uint64_t *out_num_rx_received, uint64_t *out_num_tx_burst, uint64_t *out_num_tx_sent, uint64_t *out_num_tx_dropped);

struct rte_mbuf *
mehcached_clone_packet(struct rte_mbuf *mbuf_src);

bool
mehcached_init_network(uint64_t cpu_mask, uint64_t port_mask, uint8_t *out_num_ports);

void
mehcached_free_network(uint64_t port_mask);

bool
mehcached_set_dst_port_mask(uint8_t port_id, uint16_t l4_dst_port_mask);

bool
mehcached_set_dst_port_mapping(uint8_t port_id, uint16_t l4_dst_port, uint32_t lcore);
