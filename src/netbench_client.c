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
#ifdef MEHCACHED_USE_IB
#include <netinet/ip.h>
#endif

#include "mehcached.h"
#include "hash.h"
#include "zipf.h"
//#include "trace.h"
#include "stopwatch.h"
#include "netbench_config.h"
#include "net_common.h"
#include "proto.h"
#include "netbench_hot_item_hash.h"
#include <rte_launch.h>
#include <rte_eal.h>
#include <rte_ethdev.h>
#include <rte_lcore.h>
#include <rte_byteorder.h>
#include <rte_log.h>
#include <rte_malloc.h>
#include <rte_debug.h>

// enable only one of the cluster settings
//#define MEHCACHED_CMU_XIA_ROUTERS
//#define MEHCACHED_EMULAB_C6220
//#define MEHCACHED_INTEL_ONE_DOMAIN
//#define MEHCACHED_INTEL_TWO_DOMAINS
#define MEHCACHED_INTEL_UNIFIED
//#define MEHCACHED_INTEL_IB_ONE_DOMAIN
//#define MEHCACHED_INTEL_IB_TWO_DOMAINS

#ifdef MEHCACHED_USE_IB
#include "../ib-dpdk/hrd.h"
#endif

#ifndef MEHCACHED_USE_IB
#if defined(MEHCACHED_CMU_XIA_ROUTERS)
// xia-router0/1's RX is slow
#define RX_SAMPLE_RATE (64)		// must be a power of 2
#else
//#define RX_SAMPLE_RATE (1)		// must be a power of 2
#define RX_SAMPLE_RATE (64)		// must be a power of 2
#endif
#else
// IB receives every response for flow control
#define RX_SAMPLE_RATE (1)
#endif

#ifdef MEHCACHED_USE_IB
#define TX_MAX_OUTSTANDING (16 + HRD_SS_WINDOW)
#define MAX_OUTSTANDING_REQUESTS (16)		// client may have multiple in-flight packets without responses
#endif

struct packet_construction_state
{
#ifndef MEHCACHED_USE_IB
	struct rte_mbuf *mbuf;
#else
	struct hrd_mbuf *mbuf;
#endif

	uint8_t next_index;
	uint8_t *next_key;
};

struct client_state
{
    uint8_t thread_id;
    struct mehcached_client_conf *client_conf;
    struct mehcached_workload_conf *workload_conf;
    struct mehcached_server_conf *server_conf;

	struct mehcached_hot_item_hash hot_item_hash;
	uint8_t header_template[sizeof(struct ether_hdr) + sizeof(struct ipv4_hdr) + sizeof(struct udp_hdr)];

	// runtime state
	struct packet_construction_state constr_state_e[MEHCACHED_MAX_PARTITIONS];

	uint8_t partition_id_to_thread_id[MEHCACHED_MAX_PARTITIONS];
	struct packet_construction_state constr_state_c[MEHCACHED_MAX_PARTITIONS];

	struct packet_construction_state constr_state_h[MEHCACHED_MAX_HOT_ITEMS];

#ifdef MEHCACHED_USE_IB
	struct hrd_ctrl_blk *cb[MEHCACHED_MAX_PORTS];
	struct hrd_mbuf *tx_pkts[TX_MAX_OUTSTANDING];
	uint8_t next_tx_pkt;
	uint16_t outstanding_requests;
#endif

	uint8_t next_port_index_tx;	// need modulo to get actual port index
	uint8_t next_port_index_rx;	// need modulo to get actual port index
	uint8_t next_thread_id_for_spread_requests;	// need modulo to get actual thread_id

	uint64_t num_packets_initialized;
    uint64_t num_operations_done;
    uint64_t num_key0_operations_done;
    uint64_t num_operations_succeeded;
    uint64_t num_tx_sent;
    uint64_t num_tx_dropped;
    uint64_t bytes_rx;
    uint64_t bytes_tx;
    uint64_t last_num_operations_done;
    uint64_t last_num_key0_operations_done;
    uint64_t last_num_operations_succeeded;
    uint64_t last_num_tx_sent;
    uint64_t last_num_tx_dropped;
    uint64_t last_bytes_rx;
    uint64_t last_bytes_tx;

	struct zipf_gen_state gen_state;
    uint64_t rx_sample_rand_state;
    uint64_t rx_sample_v;

    uint32_t target_request_rate_at_server;
    uint32_t target_request_rate;

    int cpu_mode;
    int port_mode;

#ifdef MEHCACHED_MEASURE_LATENCY
    int record_latency;
    // 0--128
    uint32_t latency_bin0[128];
    // 128--384
    uint32_t latency_bin1[128];
    // 384--896
    uint32_t latency_bin2[128];
    // 896--1920
    uint32_t latency_bin3[128];
    // overflow
    uint32_t latency_bin4;
#endif
} __rte_cache_aligned;

static volatile bool exiting = false;

static
void
signal_handler(int signum)
{
    if (signum == SIGINT)
        fprintf(stderr, "caught SIGINT\n");
    else if (signum == SIGTERM)
        fprintf(stderr, "caught SIGTERM\n");
    else
        fprintf(stderr, "caught unknown signal\n");
    exiting = true;
}

static
uint64_t
mehcached_hash_key(uint64_t int_key)
{
	return hash((const uint8_t *)&int_key, 8);
}

static
uint16_t
mehcached_calc_ip_checksum(struct ipv4_hdr *ip)
{
	uint16_t *ptr16;
    uint32_t ip_cksum;

    ptr16 = (uint16_t *)ip;
    ip_cksum = 0;
    ip_cksum += ptr16[0]; ip_cksum += ptr16[1];
    ip_cksum += ptr16[2]; ip_cksum += ptr16[3];
    ip_cksum += ptr16[4];
    ip_cksum += ptr16[6]; ip_cksum += ptr16[7];
    ip_cksum += ptr16[8]; ip_cksum += ptr16[9];

    ip_cksum = ((ip_cksum & 0xffff0000) >> 16) + (ip_cksum & 0x0000ffff);
    if (ip_cksum > 65535)
        ip_cksum -= 65535;
    ip_cksum = (~ip_cksum) & 0x0000ffff;
    if (ip_cksum == 0)
        ip_cksum = 0xffff;
    return (uint16_t)ip_cksum;
}

static
void
mehcached_update_ip_checksum(struct ipv4_hdr *ip, uint16_t old_v, uint16_t new_v)
{
	// rfc1624
	ip->hdr_checksum = (uint16_t)(~(~ip->hdr_checksum + ~old_v + new_v));
}

static
void
mehcached_init_header_template(struct client_state *state)
{
	memset(state->header_template, 0, sizeof(state->header_template));

	struct ether_hdr *eth = (struct ether_hdr *)state->header_template;
	struct ipv4_hdr *ip = (struct ipv4_hdr *)((unsigned char *)eth + sizeof(struct ether_hdr));
	struct udp_hdr *udp = (struct udp_hdr *)((unsigned char *)ip + sizeof(struct ipv4_hdr));

	eth->ether_type = rte_cpu_to_be_16(ETHER_TYPE_IPv4);

	ip->version_ihl = 0x40 | 0x05;
	ip->type_of_service = 0;
	ip->packet_id = 0;
	ip->fragment_offset = 0;
	ip->time_to_live = 64;
	ip->next_proto_id = IPPROTO_UDP;
	ip->hdr_checksum = 0;

	udp->dgram_cksum = 0;

	ip->hdr_checksum = mehcached_calc_ip_checksum(ip);
}

static
void
mehcached_remote_check_response(struct client_state *state)
{
	uint8_t thread_id = (uint8_t)rte_lcore_id();
	struct mehcached_workload_thread_conf *thread_conf = &state->workload_conf->threads[thread_id];

	assert(thread_conf->num_ports != 0);

	uint8_t port_id = thread_conf->port_ids[state->next_port_index_rx % thread_conf->num_ports];

#ifdef MEHCACHED_MEASURE_LATENCY
	uint32_t t_now = (uint32_t)mehcached_stopwatch_now();
	// increment next_port_index_rx so that a tight loop calling mehcached_remote_check_response() can check all ports
	// state->next_port_index_rx++;
#endif

#ifdef MEHCACHED_USE_IB
	// printf("outstanding_requests %hu\n", state->outstanding_requests);
#endif

    int i;
    for (i = 0; i < 256; i++)
    {
#ifndef MEHCACHED_USE_IB
	    struct rte_mbuf *mbuf = mehcached_receive_packet(port_id);
#else
		struct hrd_mbuf *mbuf = NULL;
		hrd_rx_burst(state->cb[port_id], &mbuf, 1);
#endif
	    if (mbuf == NULL)
	    	break;

#ifdef MEHCACHED_USE_IB
		assert(state->outstanding_requests > 0);
		state->outstanding_requests = (uint16_t)(state->outstanding_requests - 1);
#endif
	    state->bytes_rx += (uint64_t)(mbuf->data_len + 24);	// 24 for PHY overheads

#ifndef MEHCACHED_USE_IB
		struct mehcached_batch_packet *packet = rte_pktmbuf_mtod(mbuf, struct mehcached_batch_packet *);
#else
		struct mehcached_batch_packet *packet = (struct mehcached_batch_packet *)hrd_pktmbuf_mtod(mbuf);
#endif
	    const uint8_t *next_key = packet->data + sizeof(struct mehcached_request) * (size_t)packet->num_requests;
		uint8_t request_index;
		for (request_index = 0; request_index < packet->num_requests; request_index++)
		{
			struct mehcached_request *req = (struct mehcached_request *)packet->data + request_index;

			if (req->result == MEHCACHED_OK)
			{
// #ifndef MEHCACHED_MEASURE_LATENCY
				state->num_operations_succeeded += RX_SAMPLE_RATE;
// #else
// 				if (thread_id == 0)
// 					state->num_operations_succeeded += 1;
// 				else
// 					state->num_operations_succeeded += RX_SAMPLE_RATE;
// #endif
				if (req->operation == MEHCACHED_GET)
				{
					uint64_t v = *(uint64_t *)(next_key + MEHCACHED_ROUNDUP8(MEHCACHED_KEY_LENGTH(req->kv_length_vec)));
					if ((v & 0xffffffff) != ((~v >> 32) & 0xffffffff))
					{
						static unsigned int p = 0;
	                    if ((p++ & 63) == 0)
							fprintf(stderr, "thread %hhu unexpected value returned: %lu %lu\n", thread_id, (v & 0xffffffff), ((~v >> 32) & 0xffffffff));
					}
				}
			}
	        next_key += MEHCACHED_ROUNDUP8(MEHCACHED_KEY_LENGTH(req->kv_length_vec)) + MEHCACHED_ROUNDUP8(MEHCACHED_VALUE_LENGTH(req->kv_length_vec));

#ifdef MEHCACHED_MEASURE_LATENCY
			if (request_index == 0 && state->record_latency)
			{
				uint32_t diff = t_now - req->expire_time;
				uint64_t latency = mehcached_stopwatch_diff_in_us(diff, 0);

				if (latency < 128)
					state->latency_bin0[latency]++;
				else if (latency < 384)
					state->latency_bin1[(latency - 128) / 2]++;
				else if (latency < 896)
					state->latency_bin2[(latency - 384) / 4]++;
				else if (latency < 1920)
					state->latency_bin3[(latency - 896) / 8]++;
				else
					state->latency_bin4++;
			}
#endif
		}

#ifdef MEHCACHED_ENABLE_THROTTLING
		state->target_request_rate_at_server = packet->opaque;
#endif

#ifndef MEHCACHED_USE_IB
		mehcached_packet_free(mbuf);
#endif
	}
}

static
bool
mehcached_need_to_init_packet(struct client_state *state MEHCACHED_UNUSED, struct packet_construction_state *constr_state)
{
	return constr_state->mbuf == NULL;
}

static
void
mehcached_init_packet(struct client_state *state, struct packet_construction_state *constr_state, uint8_t batch_size, uint16_t mapping_id)
{
	uint8_t thread_id = (uint8_t)rte_lcore_id();

#ifndef MEHCACHED_USE_IB
	struct rte_mbuf *mbuf = mehcached_packet_alloc();
#else
	struct hrd_mbuf *mbuf = state->tx_pkts[state->next_tx_pkt];
	if (++state->next_tx_pkt == TX_MAX_OUTSTANDING)
		state->next_tx_pkt = 0;
#endif
	assert(mbuf != NULL);

#ifndef MEHCACHED_USE_IB
	struct ether_hdr *eth = rte_pktmbuf_mtod(mbuf, struct ether_hdr *);
#else
	struct ether_hdr *eth = (struct ether_hdr *)hrd_pktmbuf_mtod(mbuf);
#endif
	struct ipv4_hdr *ip = (struct ipv4_hdr *)((unsigned char *)eth + sizeof(struct ether_hdr));
	struct udp_hdr *udp = (struct udp_hdr *)((unsigned char *)ip + sizeof(struct ipv4_hdr));

	rte_memcpy(eth, state->header_template, sizeof(state->header_template));

	struct mehcached_batch_packet *packet = (struct mehcached_batch_packet *)eth;

	udp->src_port = rte_cpu_to_be_16((uint16_t)thread_id);
	udp->dst_port = rte_cpu_to_be_16(mapping_id);

	packet->num_requests = batch_size;
	packet->reserved0 = 0;
	packet->opaque = 0;

#ifndef MEHCACHED_USE_IB
	mbuf->next = NULL;
	mbuf->nb_segs = 1;
	mbuf->ol_flags = 0;
#endif

	assert(constr_state->mbuf == NULL);
	constr_state->mbuf = mbuf;
	constr_state->next_index = 0;
	constr_state->next_key = packet->data + sizeof(struct mehcached_request) * (size_t)batch_size;

	state->num_packets_initialized++;
}

static
void
mehcached_append_request(struct client_state *state MEHCACHED_UNUSED, struct packet_construction_state *constr_state, uint8_t operation, uint64_t key_hash, const uint8_t *key, size_t key_length, const uint8_t *value, size_t value_length, uint32_t expire_time)
{
#ifndef MEHCACHED_USE_IB
	struct mehcached_batch_packet *packet = rte_pktmbuf_mtod(constr_state->mbuf, struct mehcached_batch_packet *);
#else
	struct mehcached_batch_packet *packet = (struct mehcached_batch_packet *)hrd_pktmbuf_mtod(constr_state->mbuf);
#endif

	assert(constr_state->next_index < packet->num_requests);

	struct mehcached_request *req = (struct mehcached_request *)packet->data + constr_state->next_index;

	req->operation = operation;
	req->result = MEHCACHED_NOT_PROCESSED;
	req->reserved0 = 0;
	req->kv_length_vec = MEHCACHED_KV_LENGTH_VEC(key_length, value_length);
	req->key_hash = key_hash;
	req->expire_time = expire_time;
	req->reserved1 = 0;
	mehcached_memcpy8(constr_state->next_key, key, MEHCACHED_ROUNDUP8(key_length));
	constr_state->next_key += MEHCACHED_ROUNDUP8(key_length);
	mehcached_memcpy8(constr_state->next_key, value, MEHCACHED_ROUNDUP8(value_length));
	constr_state->next_key += MEHCACHED_ROUNDUP8(value_length);

#ifdef MEHCACHED_MEASURE_LATENCY
	if (constr_state->next_index == 0)
		req->expire_time = (uint32_t)mehcached_stopwatch_now();
#endif

	constr_state->next_index++;
}

static
bool
mehcached_need_to_send_packet(struct client_state *state MEHCACHED_UNUSED, struct packet_construction_state *constr_state)
{
	assert(constr_state->mbuf != NULL);
#ifndef MEHCACHED_USE_IB
	struct mehcached_batch_packet *packet = rte_pktmbuf_mtod(constr_state->mbuf, struct mehcached_batch_packet *);
#else
	struct mehcached_batch_packet *packet = (struct mehcached_batch_packet *)hrd_pktmbuf_mtod(constr_state->mbuf);
#endif

	return constr_state->next_index == packet->num_requests;
}

static
void
mehcached_client_send_packet(struct client_state *state, struct packet_construction_state *constr_state, uint8_t port_id)
{
	assert(constr_state->mbuf != NULL);
	assert(mehcached_need_to_send_packet(state, constr_state));

#ifndef MEHCACHED_USE_IB
	struct ether_hdr *eth = rte_pktmbuf_mtod(constr_state->mbuf, struct ether_hdr *);
#else
	struct ether_hdr *eth = (struct ether_hdr *)hrd_pktmbuf_mtod(constr_state->mbuf);
#endif
	struct ipv4_hdr *ip = (struct ipv4_hdr *)((unsigned char *)eth + sizeof(struct ether_hdr));
	struct udp_hdr *udp = (struct udp_hdr *)((unsigned char *)ip + sizeof(struct ipv4_hdr));

	struct mehcached_batch_packet *packet = (struct mehcached_batch_packet *)eth;

    // update stats
    uint8_t request_index;
    const uint8_t *next_key = packet->data + sizeof(struct mehcached_request) * (size_t)packet->num_requests;
    for (request_index = 0; request_index < packet->num_requests; request_index++)
    {
        struct mehcached_request *req = (struct mehcached_request *)packet->data + request_index;

		state->num_operations_done++;

		if (*(const uint64_t *)next_key == 1 && MEHCACHED_KEY_LENGTH(req->kv_length_vec) <= 8)	// for little-endian
			state->num_key0_operations_done++;

        next_key += MEHCACHED_ROUNDUP8(MEHCACHED_KEY_LENGTH(req->kv_length_vec)) + MEHCACHED_ROUNDUP8(MEHCACHED_VALUE_LENGTH(req->kv_length_vec));
	}

	// discover which server port will receive this packet to set the destination MAC correctly
	// this matters only when no switch is used
	uint8_t server_port_id;
#ifdef MEHCACHED_CMU_XIA_ROUTERS
	// XXX: CMU testbed - hardcode to detect the destination server port
	if (state->client_conf->ports[0].ip_addr[3] < 12)	// client0?
	{
		const uint8_t sp[] = {0, 1, 4, 5};
		server_port_id = sp[port_id];
	}
	else
	{
		const uint8_t sp[] = {2, 3, 6, 7};
		server_port_id = sp[port_id];
	}
#endif
#ifdef MEHCACHED_EMULAB_C6220
	// XXX: Emulab Apt c6220 - single-port machines
	server_port_id = 0;
#endif
#if defined(MEHCACHED_INTEL_ONE_DOMAIN) || defined(MEHCACHED_INTEL_TWO_DOMAINS) || defined(MEHCACHED_INTEL_UNIFIED)
	// XXX: Intel testbed - one client machine, the same port count
	// server port i <-> client port i
	server_port_id = port_id;
#endif
#if defined(MEHCACHED_INTEL_IB_ONE_DOMAIN) || defined(MEHCACHED_INTEL_IB_TWO_DOMAINS)
	// non -ib is not implemented
	server_port_id = 0;
#endif

#ifdef MEHCACHED_USE_IB
	int sn = 0;
	{
		uint16_t mapping_id = rte_be_to_cpu_16(udp->dst_port);

		// we support EREW only right now
		assert(mapping_id < 1024);
		uint16_t partition_id = mapping_id;

		uint8_t server_owner_thread_id = state->server_conf->partitions[partition_id].thread_id;

		uint8_t server_thread_id;
		for (server_thread_id = 0; server_thread_id < server_owner_thread_id; server_thread_id++)
			sn += (int)state->server_conf->threads[server_thread_id].num_ports;

		// multiple across server ports
		int server_port_index = (int)state->num_packets_initialized % state->server_conf->threads[server_owner_thread_id].num_ports;
		sn += server_port_index;
		assert(sn < state->cb[port_id]->num_remote_qps);

		// just for MAC/IP address setting (not really important in IB)
		server_port_id = state->server_conf->threads[server_owner_thread_id].port_ids[server_port_index];
	}
#endif

	rte_memcpy(&eth->s_addr, state->client_conf->ports[port_id].mac_addr, 6);
	rte_memcpy(&eth->d_addr, state->server_conf->ports[server_port_id].mac_addr, 6);

// #ifdef MEHCACHED_MEASURE_LATENCY
// 	if (rte_lcore_id() == 0)
// 	{
// 		// check response always on core 0
// 		mehcached_remote_check_response(state);
// 		// increment next_port_index_rx so that a tight loop calling mehcached_remote_check_response() can check all ports
// 		state->next_port_index_rx++;
// 	}
// 	else
// 	{
// #endif
	if ((state->num_packets_initialized & (RX_SAMPLE_RATE - 1)) == 0)
	{
		// change rx_sample_v every RX_SAMPLE_RATE TX packets so that we can sample random RX packets within the batch
		state->rx_sample_v = mehcached_rand(&state->rx_sample_rand_state) & (RX_SAMPLE_RATE - 1);
	}
	if ((state->num_packets_initialized & (RX_SAMPLE_RATE - 1)) == state->rx_sample_v)
	{
#ifndef MEHCACHED_USE_IB
		uint8_t thread_id = (uint8_t)rte_lcore_id();
		struct mehcached_workload_thread_conf *thread_conf = &state->workload_conf->threads[thread_id];
		if (thread_conf->num_ports != 0)
			mehcached_remote_check_response(state);
#else
		// mehcached_remote_check_response() will be called in the main loop
#endif
	}
	else
	{
		// use bogus source MAC address to avoid retrieval of this packet
		eth->s_addr.addr_bytes[3] = 0xff;
	}
// #ifdef MEHCACHED_MEASURE_LATENCY
// 	}
// #endif

	rte_memcpy(&ip->src_addr, state->client_conf->ports[port_id].ip_addr, 4);
	rte_memcpy(&ip->dst_addr, state->server_conf->ports[server_port_id].ip_addr, 4);

#ifdef MEHCACHED_USE_IB
	constr_state->mbuf->d_lid = state->cb[port_id]->remote_qp_attrs[sn].lid;
	constr_state->mbuf->d_qpn = state->cb[port_id]->remote_qp_attrs[sn].qpn;
#endif

	uint16_t packet_length = (uint16_t)(constr_state->next_key - (uint8_t *)packet);
	ip->total_length = rte_cpu_to_be_16((uint16_t)(packet_length - sizeof(struct ether_hdr)));

	// assume the previous checksum was calculated with both IP addresses = 0 and total_length = 0
	mehcached_update_ip_checksum(ip, 0, (uint16_t)(ip->src_addr >> 16));
	mehcached_update_ip_checksum(ip, 0, (uint16_t)(ip->src_addr >> 0));
	mehcached_update_ip_checksum(ip, 0, (uint16_t)(ip->dst_addr >> 16));
	mehcached_update_ip_checksum(ip, 0, (uint16_t)(ip->dst_addr >> 0));
	mehcached_update_ip_checksum(ip, 0, ip->total_length);

	udp->dgram_len = rte_cpu_to_be_16((uint16_t)(packet_length - sizeof(struct ether_hdr) - sizeof(struct ipv4_hdr)));

	constr_state->mbuf->data_len = (uint16_t)packet_length;
#ifndef MEHCACHED_USE_IB
	constr_state->mbuf->pkt_len = (uint32_t)packet_length;
#endif

#ifndef NDEBUG
#ifndef MEHCACHED_USE_IB
    rte_mbuf_sanity_check(constr_state->mbuf, 1);
    assert(rte_pktmbuf_headroom(constr_state->mbuf) + constr_state->mbuf->data_len <= constr_state->mbuf->buf_len);
#else
	assert(constr_state->mbuf->data_len <= HRD_MAX_DATA);
#endif
#endif

#ifndef MEHCACHED_USE_IB
	mehcached_send_packet(port_id, constr_state->mbuf);
#else
	//printf("tx sn %d lid %d sqn %d len %hu\n", sn, constr_state->mbuf->d_lid, constr_state->mbuf->d_qpn, packet_length);

	hrd_tx_burst(state->cb[port_id], &constr_state->mbuf, 1);
#endif
	constr_state->mbuf = NULL;

    state->bytes_tx += (uint64_t)(packet_length + 24);	// 24 for PHY overheads
}

static
void
mehcached_remote_schedule_request(struct client_state *state, uint8_t operation, uint64_t key_hash, const uint8_t *key, size_t key_length, const uint8_t *value, size_t value_length, uint32_t expire_time)
{
	uint8_t thread_id = (uint8_t)rte_lcore_id();
	struct mehcached_workload_thread_conf *thread_conf = &state->workload_conf->threads[thread_id];

	assert(thread_conf->num_ports != 0);

	// uint32_t opeque = (uint32_t)state->num_operations_done;

    //uint16_t partition_id = (uint16_t)(key_hash >> 48) & (uint16_t)(state->server_conf->num_partitions - 1);
	// for non power-of-two num_partitions
    uint16_t partition_id = (uint16_t)(key_hash >> 48) % (uint16_t)state->server_conf->num_partitions;

	uint16_t mapping_id = (uint16_t)-1;
	bool spread_requests = false;

#ifndef MEHCACHED_USE_SOFT_FDIR
	uint8_t hot_item_id = mehcached_get_hot_item_id(state->server_conf, &state->hot_item_hash, key_hash);
	if (hot_item_id != (uint8_t)-1)
	{
		// force CREW for hot items
		if (operation == MEHCACHED_NOOP_READ || operation == MEHCACHED_GET)
			spread_requests = true;
		else
			mapping_id = (uint16_t)(2048 + hot_item_id);
	}
	else if (operation == MEHCACHED_NOOP_READ || operation == MEHCACHED_GET)
	{
		// assume read-only (no LRU)
		if (MEHCACHED_CONCURRENT_TABLE_READ(state->server_conf, partition_id))
			spread_requests = true;
		else
			mapping_id = partition_id;
	}
	else
	{
		if (MEHCACHED_CONCURRENT_TABLE_WRITE(state->server_conf, partition_id))
			spread_requests = true;
		else
			mapping_id = partition_id;
	}
#else
	spread_requests = true;
#endif

	if (spread_requests)
	{
		if (state->partition_id_to_thread_id[partition_id] == (uint8_t)-1)
		{
			assert(mapping_id == (uint16_t)-1);
			while (true)
			{
				if (thread_conf->partition_mode >= 0)
					// XXX: this supports only two partition_mode values
					mapping_id = (uint16_t)(1024 + ((state->next_thread_id_for_spread_requests % ((uint64_t)state->server_conf->num_threads >> 1)) << 1) + (uint64_t)(partition_id & 1));
				else
					mapping_id = (uint16_t)(1024 + state->next_thread_id_for_spread_requests % state->server_conf->num_threads);
				state->next_thread_id_for_spread_requests++;
				// we choose this mapping id only when the corresponding thread is active (i.e., uses any port)
				if (state->server_conf->threads[mapping_id - 1024].num_ports > 0)
					break;
			}
			state->partition_id_to_thread_id[partition_id] = (uint8_t)(mapping_id - 1024);
		}
		else
			// reuse the previous mapping
			mapping_id = (uint16_t)(1024 + state->partition_id_to_thread_id[partition_id]);
	}
	assert(mapping_id != (uint16_t)-1);

	struct packet_construction_state *constr_state;
	if (mapping_id < 1024)
		constr_state = state->constr_state_e + partition_id;
	else if (mapping_id < 2048)
		constr_state = state->constr_state_c + partition_id;
	else
		constr_state = state->constr_state_h + (mapping_id - 2048);

	if (mehcached_need_to_init_packet(state, constr_state))
		mehcached_init_packet(state, constr_state, thread_conf->batch_size, mapping_id);

	mehcached_append_request(state, constr_state, operation, key_hash, key, key_length, value, value_length, expire_time);

	if (mehcached_need_to_send_packet(state, constr_state))
	{
		// choose which client port to use to send a request for a particular server partition
		uint8_t port_id;

#ifdef MEHCACHED_CMU_XIA_ROUTERS
		// XXX: assume that the top half is for (mapping_id & 1) == 0 and the bottom half is for (mapping_id & 1) == 1
		if (spread_requests)
			port_id = thread_conf->port_ids[(thread_conf->num_ports >> 1) * (mapping_id & 1) + state->next_port_index_tx % (thread_conf->num_ports >> (1 + state->port_mode))];
		else
		{
			//port_id = thread_conf->port_ids[(thread_conf->num_ports >> 1) * (partition_id & 1) + state->next_port_index_tx % (thread_conf->num_ports >> 1)];
			// this following allows putting all partitions to one thread (MEMCACHED) rather than sending requests to both NUMA domains
			port_id = thread_conf->port_ids[(thread_conf->num_ports >> 1) * (state->server_conf->partitions[partition_id].thread_id & 1) + state->next_port_index_tx % (thread_conf->num_ports >> (1 + state->port_mode))];
		}
#endif
#ifdef MEHCACHED_EMULAB_C6220
		// XXX: for a single-port machine (Emulab Apt)
		port_id = thread_conf->port_ids[0];
#endif
#ifdef MEHCACHED_INTEL_ONE_DOMAIN
        // XXX: this block can be combined with MEHCACHED_INTEL_UNIFIED, but kept to avoid any regression
		// XXX: the server has one domain - sending a packet to any server will arrive at any server core
		port_id = thread_conf->port_ids[state->next_port_index_tx % (thread_conf->num_ports >> (0 + state->port_mode))];
#endif
#ifdef MEHCACHED_INTEL_TWO_DOMAINS
        // XXX: this block can be combined with MEHCACHED_INTEL_UNIFIED, but kept to avoid any regression
		if (spread_requests)
		{
			assert(mapping_id >= 1024);
			uint8_t server_thread_id = (uint8_t)(mapping_id - 1024);
			uint8_t server_port_idx = state->next_port_index_tx % state->server_conf->threads[server_thread_id].num_ports;
			uint8_t server_port_id = state->server_conf->threads[server_thread_id].port_ids[server_port_idx];
			// server port i <-> client port i
			// XXX: this may use a client port that is not registered in the client machine configuration
			port_id = server_port_id;
		}
		else
		{
			assert(mapping_id < 1024);
			// for partition_id < num_partitions / 2 (server node 0), use the first half client ports
			// for partition_id >= num_partitions / 2 (server node 1), use the second half client ports
			if (mapping_id < (state->server_conf->num_partitions >> 1))
				port_id = thread_conf->port_ids[(thread_conf->num_ports >> 1) * 0 + state->next_port_index_tx % (thread_conf->num_ports >> (1 + state->port_mode))];
			else
				port_id = thread_conf->port_ids[(thread_conf->num_ports >> 1) * 1 + state->next_port_index_tx % (thread_conf->num_ports >> (1 + state->port_mode))];
		}
#endif
#ifdef MEHCACHED_INTEL_UNIFIED
		uint8_t server_thread_id;
		if (spread_requests)
		{
			assert(mapping_id >= 1024);
			server_thread_id = (uint8_t)(mapping_id - 1024);
		}
		else
		{
			assert(mapping_id < 1024);
			server_thread_id = state->server_conf->partitions[partition_id].thread_id;
		}

		uint8_t server_port_idx = state->next_port_index_tx % state->server_conf->threads[server_thread_id].num_ports;
		uint8_t server_port_id = state->server_conf->threads[server_thread_id].port_ids[server_port_idx];
		// server port i <-> client port i
		// XXX: this may use a client port that is not registered in the client machine configuration
		port_id = server_port_id;
#endif
#if defined(MEHCACHED_INTEL_IB_ONE_DOMAIN) || defined(MEHCACHED_INTEL_IB_TWO_DOMAINS)
		// XXX: Intel clients are single-port machines
		port_id = thread_conf->port_ids[0];
#endif

		mehcached_client_send_packet(state, constr_state, port_id);

		if (spread_requests)
			// reset this so that we can pick a different mapping for the partition
			state->partition_id_to_thread_id[partition_id] = (uint8_t)-1;
	}
}

static
void
mehcached_remote_noop_read(struct client_state *state, uint64_t key_hash, const uint8_t *key, size_t key_length)
{
	mehcached_remote_schedule_request(state, MEHCACHED_NOOP_READ, key_hash, key, key_length, NULL, 0, 0);
}

static
void
mehcached_remote_noop_write(struct client_state *state, uint64_t key_hash, const uint8_t *key, size_t key_length, const uint8_t *value, size_t value_length, uint32_t expire_time)
{
	mehcached_remote_schedule_request(state, MEHCACHED_NOOP_WRITE, key_hash, key, key_length, value, value_length, expire_time);
}

static
void
mehcached_remote_set(struct client_state *state, uint64_t key_hash, const uint8_t *key, size_t key_length, const uint8_t *value, size_t value_length, uint32_t expire_time, bool overwrite)
{
	mehcached_remote_schedule_request(state, overwrite ? MEHCACHED_SET : MEHCACHED_ADD, key_hash, key, key_length, value, value_length, expire_time);
}

static
void
mehcached_remote_get(struct client_state *state, uint64_t key_hash, const uint8_t *key, size_t key_length)
{
	mehcached_remote_schedule_request(state, MEHCACHED_GET, key_hash, key, key_length, NULL, 0, 0);
}

static
void
mehcached_remote_test(struct client_state *state, uint64_t key_hash, const uint8_t *key, size_t key_length)
{
	mehcached_remote_schedule_request(state, MEHCACHED_TEST, key_hash, key, key_length, NULL, 0, 0);
}

static
void
mehcached_remote_delete(struct client_state *state, uint64_t key_hash, const uint8_t *key, size_t key_length)
{
	mehcached_remote_schedule_request(state, MEHCACHED_DELETE, key_hash, key, key_length, NULL, 0, 0);
}

static
void
mehcached_remote_increment(struct client_state *state, uint64_t key_hash, const uint8_t *key, size_t key_length, uint64_t increment, uint32_t expire_time)
{
	mehcached_remote_schedule_request(state, MEHCACHED_INCREMENT, key_hash, key, key_length, (const uint8_t *)&increment, sizeof(uint64_t), expire_time);
}

static
int
mehcached_benchmark_client_proc(void *arg)
{
    struct client_state **states = (struct client_state **)arg;

    uint8_t thread_id = (uint8_t)rte_lcore_id();
    struct client_state *state = states[thread_id];
	struct mehcached_client_conf *client_conf = state->client_conf;
	struct mehcached_workload_thread_conf *thread_conf = &state->workload_conf->threads[thread_id];


	mehcached_calc_hot_item_hash(state->server_conf, &state->hot_item_hash);

#ifdef MEHCACHED_USE_IB
	// XXX: this may prevent using multiple workloads by reinitializing ctrl_blk
	// this must be initialized here because of its protection domain;
	// if this is done too early, some place in hrd_tx_burst() will cause a segfault
	static volatile uint8_t ib_init_thread_id = 0;
	while (ib_init_thread_id != thread_id)
		;

	char addr[1024];
	FILE *fp = fopen("conf_ib_memcached_address", "r");
	if (fp == NULL) {
		printf("cannot open file: conf_ib_memcached_address\n");
		return 0;
	}
	if (fgets(addr, sizeof(addr), fp) == NULL) {
		printf("cannot read file: conf_ib_memcached_address\n");
		return 0;
	}
	while (addr[strlen(addr)] == '\n')
		addr[strlen(addr)] = '\0';
	fclose(fp);
	
	{
        uint8_t port_index;
        for (port_index = 0; port_index < thread_conf->num_ports; port_index++) {
			uint8_t port_id = thread_conf->port_ids[port_index];
			state->cb[port_id] = hrd_init_ctrl_blk(thread_id * MEHCACHED_MAX_PORTS + port_id, port_id, (int)rte_lcore_to_socket_id((unsigned int)thread_id));
			state->cb[port_id]->num_remote_qps = hrd_get_registered_qps(state->cb[port_id], addr);
		}
	}

	ib_init_thread_id++;
#endif


	uint64_t key[MEHCACHED_ROUNDUP8(thread_conf->key_length) / 8 + 1];
	uint64_t value[MEHCACHED_ROUNDUP8(thread_conf->value_length) / 8 + 1];
	memset(key, 0, sizeof(key));
	memset(value, 0, sizeof(value));

    size_t log16_num_items = 0;
    while (((size_t)1 << (log16_num_items * 4)) < (thread_conf->num_items + 1))
        log16_num_items++;
    size_t key_position_step = thread_conf->key_length / log16_num_items;
    if (key_position_step == 0)
        key_position_step = 1;
    // printf("%lu %lu %lu\n", key_position_step, log16_num_items, thread_conf->key_length);
    assert(key_position_step * log16_num_items <= thread_conf->key_length);

	uint64_t op_type_rand_state = (uint64_t)thread_id ^ mehcached_stopwatch_now();

    uint64_t t_start;
    uint64_t t_end;
	double diff;
	double prev_report = 0.;
    double prev_rate_update = 0.;
#ifdef MEHCACHED_MEASURE_LATENCY
    double prev_latency_report = 0.;
#endif

    t_start = mehcached_stopwatch_now();

    uint64_t i = 0;

    double get_ratio = thread_conf->get_ratio;
    double put_ratio = thread_conf->put_ratio;
    double abs_get_ratio = get_ratio;
    double abs_put_ratio = put_ratio;
    if (abs_get_ratio < 0.)
    	abs_get_ratio = -abs_get_ratio;
    if (abs_put_ratio < 0.)
    	abs_put_ratio = -abs_put_ratio;

    const uint32_t get_threshold = (uint32_t)(abs_get_ratio * (double)((uint32_t)-1));
    const uint32_t put_threshold = (uint32_t)((abs_get_ratio + abs_put_ratio) * (double)((uint32_t)-1));

    int packet_index;
    int num_batch_proc_packets = 32;

	if (thread_conf->num_ports == 0)
		num_batch_proc_packets = 0;

    state->rx_sample_rand_state = (uint64_t)thread_id ^ mehcached_stopwatch_now();
    state->rx_sample_v = 0;	// will be set later

#ifdef MEHCACHED_ENABLE_THROTTLING
    uint64_t batch_t_start = mehcached_stopwatch_now();
    uint64_t batch_t_end = batch_t_start;
#endif

    // uint64_t last_ipackets[client_conf->num_ports];
    // uint64_t last_ierrors[client_conf->num_ports];
    uint64_t last_opackets[client_conf->num_ports];
    uint64_t last_oerrors[client_conf->num_ports];

    if (thread_id == 0)
    {
        uint8_t port_id;
        for (port_id = 0; port_id < client_conf->num_ports; port_id++)
        {
            // struct rte_eth_stats stats;
            // rte_eth_stats_get(port_id, &stats);

            // last_ipackets[port_id] = stats.ipackets;
            // last_ierrors[port_id] = stats.ierrors;
            // last_opackets[port_id] = stats.opackets;
            // last_oerrors[port_id] = stats.oerrors;

            uint64_t opackets = 0;
            uint64_t oerrors = 0;
            uint8_t thread_id;
            for (thread_id = 0; thread_id < state->workload_conf->num_threads; thread_id++)
            {
                uint64_t num_tx_sent;
                uint64_t num_tx_dropped;
#ifndef MEHCACHED_USE_IB
                mehcached_get_stats_lcore(port_id, thread_id, NULL, NULL, NULL, &num_tx_sent, &num_tx_dropped);
#else
				num_tx_sent = 0;
				num_tx_dropped = 0;
#endif
                opackets += num_tx_sent;
                oerrors += num_tx_dropped;
                // XXX: how to handle integer wraps after a very long run?
            }
            last_opackets[port_id] = opackets;
            last_oerrors[port_id] = oerrors;
        }
    }

    while (!exiting)
	{
		if (thread_conf->num_operations != 0 && state->num_operations_done >= thread_conf->num_operations)
			break;
#ifdef MEHCACHED_ENABLE_THROTTLING
	    uint64_t num_new_requests = 0;
#endif

		for (packet_index = 0; packet_index < num_batch_proc_packets; packet_index++)
		{
#ifdef MEHCACHED_USE_IB
			if (state->outstanding_requests < MAX_OUTSTANDING_REQUESTS)
			{
#endif

			uint32_t op_r = mehcached_rand(&op_type_rand_state);
			bool is_get = op_r <= get_threshold;
			bool is_put = op_r > get_threshold && op_r <= put_threshold;
			bool is_increment = op_r > put_threshold;

			uint64_t key_index = mehcached_zipf_next(&state->gen_state);
			assert(key_index < thread_conf->num_items);
			uint64_t key_hash = mehcached_hash_key(key_index);

			// if (valid_key)
			{
				// binary key
				//size_t key_length = thread_conf->key_length;
				//*(uint64_t *)key = key_index;

				// variable-length hexadecimal key
				size_t key_length = 0;
				{
					size_t i;
					for (i = 0; i < sizeof(key) / sizeof(key[0]); i++)
		                key[i] = 0;     // for keys that need zero paddings to an 8-byte boundary
					uint64_t key_index_copy = key_index + 1;
					while (key_index_copy > 0)
					{
						key_index_copy >>= 4;
						key_length += key_position_step;
					}
					key_index_copy = key_index + 1;
					size_t char_index = key_length;
					while (key_index_copy > 0)
					{
						char_index -= key_position_step;
						((char *)key)[char_index] = (char)(key_index_copy & 15);
						key_index_copy >>= 4;
					}
				}

				if (is_get)
				{
					if (get_ratio >= 0.)
						mehcached_remote_get(state, key_hash, (const uint8_t *)key, key_length);
					else
						mehcached_remote_noop_read(state, key_hash, (const uint8_t *)key, key_length);
				}
				else if (is_put)
				{
					*(uint64_t *)value = (i & 0xffffffff) | ((~i & 0xffffffff) << 32);
					if (put_ratio >= 0.)
						mehcached_remote_set(state, key_hash, (const uint8_t *)key, key_length, (const uint8_t *)value, thread_conf->value_length, 0, true);
					else
						mehcached_remote_noop_write(state, key_hash, (const uint8_t *)key, key_length, (const uint8_t *)value, thread_conf->value_length, 0);
				}
				else if (is_increment)
				{
					mehcached_remote_increment(state, key_hash, (const uint8_t *)key, key_length, 1, 0);
				}
				else
				{
					fprintf(stderr, "bug or invalid configuration\n");
					assert(false);
				}

#ifdef MEHCACHED_ENABLE_THROTTLING
				num_new_requests++;
#endif
#ifdef MEHCACHED_USE_IB
				state->outstanding_requests++;
#endif
			}

#ifdef MEHCACHED_USE_IB
			}
#endif

		}

#ifdef MEHCACHED_USE_IB
		// we need to call this always because the IB version does not send out any packet when there are too many outstanding requests while we need to receive packets to update the number of outstanding requests
		if (thread_conf->num_ports != 0)
			mehcached_remote_check_response(state);
#endif

		// we do this once per batch so that multiple mapping values can be used for each port
		state->next_port_index_rx++;
		state->next_port_index_tx++;

#ifdef MEHCACHED_ENABLE_THROTTLING
		uint32_t min_target_request_rate = state->target_request_rate_at_server;
		if (min_target_request_rate > state->target_request_rate)
			min_target_request_rate = state->target_request_rate;
		if (min_target_request_rate > 0)
		{
		    while (!exiting)
			{
#ifdef MEHCACHED_MEASURE_LATENCY
				if (thread_id == 0)
				{
					// check response on core 0
					if (thread_conf->num_ports != 0)
						mehcached_remote_check_response(state);
					// increment next_port_index_rx so that a tight loop calling mehcached_remote_check_response() can check all ports
					state->next_port_index_rx++;
				}
#endif

			    batch_t_end = mehcached_stopwatch_now();
			    if (batch_t_start != batch_t_end)
			    {
				    double t_diff = mehcached_stopwatch_diff_in_s(batch_t_end, batch_t_start);
				    if (t_diff >= 0.1)	// throttle no longer than 0.1 second (target_request rate may be too low)
				    	break;
				    uint32_t actual_request_rate = (uint32_t)((double)num_new_requests / t_diff);
				    if (actual_request_rate <= min_target_request_rate)
					    break;
				}

				// send out all buffered packets while waiting
#ifndef MEHCACHED_USE_IB
				size_t port_index;
	            for (port_index = 0; port_index < thread_conf->num_ports; port_index++)
	            {
	                uint8_t port_id = thread_conf->port_ids[port_index];
					mehcached_send_packet_flush(port_id);
				}
#endif
			}
			batch_t_start = batch_t_end;
		}
#endif

		i++;

        if ((i & 0xff) == 0)
        {
            uint64_t total_num_tx_sent = 0;
            uint64_t total_num_tx_dropped = 0;
		    size_t port_index;
            for (port_index = 0; port_index < thread_conf->num_ports; port_index++)
            {
                uint8_t port_id = thread_conf->port_ids[port_index];
                uint64_t num_tx_sent;
                uint64_t num_tx_dropped;
#ifndef MEHCACHED_USE_IB
                mehcached_get_stats(port_id, NULL, NULL, NULL, &num_tx_sent, &num_tx_dropped);
#else
				(void)port_id;
				num_tx_sent = 0;
				num_tx_dropped = 0;
#endif
                total_num_tx_sent += num_tx_sent;
                total_num_tx_dropped += num_tx_dropped;
            }
            state->num_tx_sent = total_num_tx_sent;
            state->num_tx_dropped = total_num_tx_dropped;

            t_end = mehcached_stopwatch_now();
            diff = mehcached_stopwatch_diff_in_s(t_end, t_start);

    		if (diff - prev_report >= 1.)
    		{
                if (thread_id == 0)
                {
        			uint64_t total_new_num_tx_sent = 0;
        			uint64_t total_new_num_tx_dropped = 0;
        			uint64_t total_new_bytes_rx = 0;
        			uint64_t total_new_bytes_tx = 0;
                    uint64_t total_new_num_operations_done = 0;
                    uint64_t total_new_num_key0_operations_done = 0;
                    uint64_t total_new_num_operations_succeeded = 0;
                    size_t num_active_threads = 0;

                    size_t thread_id;
                    for (thread_id = 0; thread_id < state->workload_conf->num_threads; thread_id++)
                    {
                        uint64_t num_tx_sent = states[thread_id]->num_tx_sent;
                        uint64_t new_num_tx_sent = num_tx_sent - states[thread_id]->last_num_tx_sent;
                        states[thread_id]->last_num_tx_sent = num_tx_sent;

                        uint64_t num_tx_dropped = states[thread_id]->num_tx_dropped;
                        uint64_t new_num_tx_dropped = num_tx_dropped - states[thread_id]->last_num_tx_dropped;
                        states[thread_id]->last_num_tx_dropped = num_tx_dropped;

                        uint64_t bytes_rx = states[thread_id]->bytes_rx;
                        uint64_t new_bytes_rx = bytes_rx - states[thread_id]->last_bytes_rx;
                        states[thread_id]->last_bytes_rx = bytes_rx;

                        uint64_t bytes_tx = states[thread_id]->bytes_tx;
                        uint64_t new_bytes_tx = bytes_tx - states[thread_id]->last_bytes_tx;
                        states[thread_id]->last_bytes_tx = bytes_tx;

                        uint64_t num_operations_done = states[thread_id]->num_operations_done;
                        uint64_t new_num_operations_done = num_operations_done - states[thread_id]->last_num_operations_done;
                        states[thread_id]->last_num_operations_done = num_operations_done;

                        uint64_t num_key0_operations_done = states[thread_id]->num_key0_operations_done;
                        uint64_t new_num_key0_operations_done = num_key0_operations_done - states[thread_id]->last_num_key0_operations_done;
                        states[thread_id]->last_num_key0_operations_done = num_key0_operations_done;

                        uint64_t num_operations_succeeded = states[thread_id]->num_operations_succeeded;
                        uint64_t new_num_operations_succeeded = num_operations_succeeded - states[thread_id]->last_num_operations_succeeded;
                        states[thread_id]->last_num_operations_succeeded = num_operations_succeeded;

                        total_new_num_tx_sent += new_num_tx_sent;
                        total_new_num_tx_dropped += new_num_tx_dropped;
                        total_new_bytes_rx += new_bytes_rx;
                        total_new_bytes_tx += new_bytes_tx;
                        total_new_num_operations_done += new_num_operations_done;
                        total_new_num_key0_operations_done += new_num_key0_operations_done;
                        total_new_num_operations_succeeded += new_num_operations_succeeded;

                        if (new_bytes_tx != 0)
                        	num_active_threads++;
                    }

#ifndef MEHCACHED_USE_IB
                    double effective_tx_ratio = 0.;
                    if (total_new_num_tx_sent + total_new_num_tx_dropped != 0)
	                    effective_tx_ratio = (double)total_new_num_tx_sent / (double)(total_new_num_tx_sent + total_new_num_tx_dropped);
#else
                    double effective_tx_ratio = 1.;
#endif

                    double success_rate = 0.;
                    if (total_new_num_operations_done != 0 && effective_tx_ratio > 0.)
                    {
                    	success_rate = (double)total_new_num_operations_succeeded / ((double)total_new_num_operations_done * effective_tx_ratio);
                    	if (success_rate > 1.)
                    		success_rate = 1.;	// total_new_num_operations_succeeded & total_new_num_operations_done are measured for a different set of requests
                    }

                    double total_mops = (double)total_new_num_operations_done * effective_tx_ratio;
                    double key0_mops;
                    if (total_new_num_operations_done != 0)
	                    key0_mops = total_mops * ((double)total_new_num_key0_operations_done / (double)total_new_num_operations_done);
	                else
	                	key0_mops = 0.;
                    total_mops *= 1. / (diff - prev_report) * 0.000001;
                    key0_mops *= 1. / (diff - prev_report) * 0.000001;
                    double gbps_rx = (double)total_new_bytes_rx / (diff - prev_report) * 8 * 0.000000001;
                    double gbps_tx = (double)total_new_bytes_tx / (diff - prev_report) * 8 * 0.000000001 * effective_tx_ratio;

                    printf("%.1f current_ops: %10.2lf Mops (%10.2lf Mops for key0); success_rate: %3.2f%%", diff, total_mops, key0_mops, success_rate * 100.);

                    printf("; bw: %.2lf Gbps (rx), %.2lf Gbps (tx); threads: %zu", gbps_rx, gbps_tx, num_active_threads);

                    printf("; target_request_rate: %.3f Mops (c), %.3f Mops (s)", (float)state->target_request_rate * 0.000001f, (float)state->target_request_rate_at_server * 0.000001f);

	                printf("\n");
	                fflush(stdout);
                }

    			prev_report = diff;
            }

#ifdef MEHCACHED_MEASURE_LATENCY
			// do not record latency for 1 second after dumping the histogram to the file
			// because latency data is distorted by the file write
    		if (diff - prev_latency_report >= 1.)
    		{
                if (thread_id == 0)
                	if (!state->record_latency)
                		state->record_latency = 1;
    		}
    		if (diff - prev_latency_report >= 10.)
    		{
                if (thread_id == 0)
                {
                	FILE *fp = fopen("output_latency.tmp", "wb");
	                uint32_t latency_bin_index;
	                for (latency_bin_index = 0; latency_bin_index < 128; latency_bin_index++)
	                	fprintf(fp, "%4u %6u\n", 0 + latency_bin_index * 1, state->latency_bin0[latency_bin_index]);
	                for (latency_bin_index = 0; latency_bin_index < 128; latency_bin_index++)
	                	fprintf(fp, "%4u %6u\n", 128 + latency_bin_index * 2, state->latency_bin1[latency_bin_index]);
	                for (latency_bin_index = 0; latency_bin_index < 128; latency_bin_index++)
	                	fprintf(fp, "%4u %6u\n", 384  + latency_bin_index * 4, state->latency_bin2[latency_bin_index]);
	                for (latency_bin_index = 0; latency_bin_index < 128; latency_bin_index++)
	                	fprintf(fp, "%4u %6u\n", 896  + latency_bin_index * 8, state->latency_bin3[latency_bin_index]);
                	fprintf(fp, "%4u %6u\n", 1920, state->latency_bin4);
                	fclose(fp);

	                memset(state->latency_bin0, 0, sizeof(state->latency_bin0));
	                memset(state->latency_bin1, 0, sizeof(state->latency_bin1));
	                memset(state->latency_bin2, 0, sizeof(state->latency_bin2));
	                memset(state->latency_bin3, 0, sizeof(state->latency_bin3));
	                state->latency_bin4 = 0;

	                printf("latency report written to output_latency.tmp\n");
	            }
                prev_latency_report = diff;
        		state->record_latency = 0;
	        }
#endif

            if (diff - prev_rate_update >= 1.0)
            {
                if (thread_id == 0)
                {
                    float max_loss = 0.;

                    uint8_t port_id;
		            for (port_id = 0; port_id < client_conf->num_ports; port_id++)
		            {
		            	uint64_t opackets = 0;
		            	uint64_t oerrors = 0;
		            	uint8_t thread_id;
                        for (thread_id = 0; thread_id < state->workload_conf->num_threads; thread_id++)
                        {
                            uint64_t num_tx_sent;
                            uint64_t num_tx_dropped;
#ifndef MEHCACHED_USE_IB
                            mehcached_get_stats_lcore(port_id, thread_id, NULL, NULL, NULL, &num_tx_sent, &num_tx_dropped);
#else
							num_tx_sent = 0;
							num_tx_dropped = 0;
#endif
			                opackets += num_tx_sent;
			                oerrors += num_tx_dropped;
			                // XXX: how to handle integer wraps after a very long run?
			            }
						//printf("port %hu %lu %lu\n", port_id, opackets, oerrors);
			            uint64_t new_opackets = opackets - last_opackets[port_id];
			            last_opackets[port_id] = opackets;
			            uint64_t new_oerrors = oerrors - last_oerrors[port_id];
			            last_oerrors[port_id] = oerrors;

			            float oloss;
                        if (new_opackets + new_oerrors != 0)
                            oloss = (float)new_oerrors / (float)(new_opackets + new_oerrors);
			           	else
			           		oloss = 0.f;

			           	if (max_loss < oloss)
			           		max_loss = oloss;
		            }

                    uint32_t new_target_request_rate;
                    if (max_loss <= 0.01f)
                        new_target_request_rate = (uint32_t)((float)state->target_request_rate * (1.f + (0.01f - max_loss)));
                    else if (max_loss <= 0.02f)
                        new_target_request_rate = (uint32_t)((float)state->target_request_rate / (1.f + (max_loss - 0.01f)));
                    else
                        new_target_request_rate = (uint32_t)((float)state->target_request_rate / (1.f + (0.02f - 0.01f)));
                    if (new_target_request_rate < 1000) // cap at 1 Kops
                        new_target_request_rate = 1000;
                    if (new_target_request_rate > 20000000) // cap at 20 Mops
                        new_target_request_rate = 20000000;

                    uint8_t thread_id;
                    for (thread_id = 0; thread_id < state->workload_conf->num_threads; thread_id++)
                        states[thread_id]->target_request_rate = new_target_request_rate;
                }

                prev_rate_update = diff;

// #ifdef MEHCACHED_MEASURE_LATENCY
//                 {
//                 	// copy thread 0's target_request_rate_at_server to other threads because only thread 0 is receiving response packets
//                     uint8_t thread_id;
//                     for (thread_id = 1; thread_id < state->workload_conf->num_threads; thread_id++)
//                         states[thread_id]->target_request_rate_at_server = states[0]->target_request_rate_at_server;
//                 }
// #endif
            }

            if (thread_conf->duration != 0. && diff >= thread_conf->duration)
            	break;
        }
	}

	return 0;
}

static
void
mehcached_benchmark_client(const char *machine_filename, const char *client_name, int cpu_mode, int port_mode, int num_workloads, const char *workload_filenames[])
{
    struct mehcached_client_conf *client_conf = mehcached_get_client_conf(machine_filename, client_name);

	mehcached_stopwatch_init_start();

    printf("initializing shm\n");

    const size_t page_size = 1048576 * 2;
    const size_t num_numa_nodes = 2;
#ifdef MEHCACHED_CMU_XIA_ROUTERS
    const size_t num_pages_to_try = 4096;
    const size_t num_pages_to_reserve = 4096 - 2048;	// give 2048 pages to dpdk
#endif
#if defined(MEHCACHED_EMULAB_C6220) || defined(MEHCACHED_INTEL_ONE_DOMAIN) || defined(MEHCACHED_INTEL_TWO_DOMAINS) || defined(MEHCACHED_INTEL_UNIFIED) || defined(MEHCACHED_INTEL_IB_ONE_DOMAIN) || defined(MEHCACHED_INTEL_IB_TWO_DOMAINS)
    const size_t num_pages_to_try = 16384;
    const size_t num_pages_to_reserve = 16384 - 2048;	// give 2048 pages to dpdk
#endif

    mehcached_shm_init(page_size, num_numa_nodes, num_pages_to_try, num_pages_to_reserve);

    printf("initializing DPDK\n");

    uint64_t cpu_mask = 0;
    uint8_t thread_id;
	for (thread_id = 0; thread_id < client_conf->num_threads; thread_id++)
		cpu_mask |= (uint64_t)1 << thread_id;
	char cpu_mask_str[100];
	snprintf(cpu_mask_str, sizeof(cpu_mask_str), "%lx", cpu_mask);

	char *rte_argv[] = {"",
		"-c", cpu_mask_str,
		//"-n", "3",	// 3 for client0/1
		"-n", "4",
	};
	int rte_argc = sizeof(rte_argv) / sizeof(rte_argv[0]);

    //rte_set_log_level(RTE_LOG_DEBUG);
    rte_set_log_level(RTE_LOG_NOTICE);

	int ret = rte_eal_init(rte_argc, rte_argv);
	if (ret < 0)
	{
		fprintf(stderr, "failed to initialize EAL\n");
		return;
	}

#ifndef MEHCACHED_USE_IB
	uint8_t num_ports_max;
	uint64_t port_mask = 0;
    uint8_t port_id;
	for (port_id = 0; port_id < client_conf->num_ports; port_id++)
		port_mask |= (uint64_t)1 << port_id;
	if (!mehcached_init_network(cpu_mask, port_mask, &num_ports_max))
	{
		fprintf(stderr, "failed to initialize network\n");
		return;
	}
	assert(client_conf->num_ports <= num_ports_max);


	printf("setting MAC address\n");
    for (port_id = 0; port_id < client_conf->num_ports; port_id++)
    {
    	struct ether_addr mac_addr;
    	memcpy(&mac_addr, client_conf->ports[port_id].mac_addr, sizeof(struct ether_addr));
    	if (rte_eth_dev_mac_addr_add(port_id, &mac_addr, 0) != 0)
    	{
			fprintf(stderr, "failed to add a MAC address\n");
			return;
    	}
    }


    printf("configuring mappings\n");

    for (port_id = 0; port_id < client_conf->num_ports; port_id++)
    {
        if (!mehcached_set_dst_port_mask(port_id, 0xffff))
            return;
    }

    for (thread_id = 0; thread_id < client_conf->num_threads; thread_id++)
    {
        for (port_id = 0; port_id < client_conf->num_ports; port_id++)
            if (!mehcached_set_dst_port_mapping(port_id, (uint16_t)thread_id, thread_id))
                return;
    }
#endif


    printf("initializing client states\n");

    struct client_state *states[client_conf->num_threads];

	for (thread_id = 0; thread_id < client_conf->num_threads; thread_id++)
        states[thread_id] = mehcached_eal_malloc_lcore(sizeof(struct client_state), thread_id);

	mehcached_stopwatch_init_end();


    printf("running clients\n");

    struct sigaction new_action;
    new_action.sa_handler = signal_handler;
    sigemptyset(&new_action.sa_mask);
    new_action.sa_flags = 0;
    sigaction(SIGINT, &new_action, NULL);
    sigaction(SIGTERM, &new_action, NULL);

#ifdef MEHCACHED_USE_IB
	assert(num_workloads == 1);
#endif

    int workload_index;
    for (workload_index = 0; workload_index < num_workloads; workload_index++)
    {
    	struct mehcached_workload_conf *workload_conf = mehcached_get_workload_conf(workload_filenames[workload_index], client_name);

	    printf("using workload %d\n", workload_index);

		for (thread_id = 0; thread_id < workload_conf->num_threads; thread_id++)
		{
	        struct client_state *state = states[thread_id];
			memset(state, 0, sizeof(struct client_state));

			state->thread_id = thread_id;
			state->client_conf = client_conf;
			state->workload_conf = workload_conf;
			state->server_conf = mehcached_get_server_conf(machine_filename, workload_conf->threads[thread_id].server_name);

#ifdef MEHCACHED_USE_IB
			int j;
			for (j = 0; j < TX_MAX_OUTSTANDING; j++)
			{
				state->tx_pkts[j] = mehcached_eal_malloc_lcore(sizeof(struct hrd_mbuf), thread_id);
				assert(state->tx_pkts[j]);
			}
			state->next_tx_pkt = 0;

			state->outstanding_requests = 0;
#endif

			mehcached_init_header_template(state);

			uint16_t partition_id;
			for (partition_id = 0; partition_id < MEHCACHED_MAX_PARTITIONS; partition_id++)
		        state->partition_id_to_thread_id[partition_id] = (uint8_t)-1;

			if (thread_id == 0 ||
				workload_conf->threads[thread_id - 1].num_items != workload_conf->threads[thread_id].num_items ||
				workload_conf->threads[thread_id - 1].zipf_theta != workload_conf->threads[thread_id].zipf_theta)
			{
				mehcached_zipf_init(&state->gen_state, workload_conf->threads[thread_id].num_items, workload_conf->threads[thread_id].zipf_theta, (thread_id ^ mehcached_stopwatch_now()) & 0xffffffffffffu);
				mehcached_zipf_next(&state->gen_state);
			}
			else
				mehcached_zipf_init_copy(&state->gen_state, &states[thread_id - 1]->gen_state, (thread_id ^ mehcached_stopwatch_now()) & 0xffffffffffffu);

	        state->target_request_rate_at_server = 20000000;  // 20 Mops
	        state->target_request_rate = 20000000;  // 20 Mops

	        state->cpu_mode = cpu_mode;
	        state->port_mode = port_mode;
		}

	    for (thread_id = 1; thread_id < workload_conf->num_threads; thread_id++)
			rte_eal_launch(mehcached_benchmark_client_proc, states, (unsigned int)thread_id);
		rte_eal_launch(mehcached_benchmark_client_proc, states, 0);

		rte_eal_mp_wait_lcore();

		for (thread_id = 0; thread_id < workload_conf->num_threads; thread_id++)
			free(states[thread_id]->server_conf);

		free(workload_conf);
	}


    // mehcached_free_network(port_mask);

    printf("finished\n");
}

int
main(int argc, const char *argv[])
{
	if (argc < 5)
	{
		printf("%s MACHINE-FILENAME CLIENT-NAME CPU-MODE PORT-MODE { WORKLOAD-FILENAME ... }\n", argv[0]);
		return EXIT_FAILURE;
	}

    mehcached_benchmark_client(argv[1], argv[2], atoi(argv[3]), atoi(argv[4]), argc - 5, argv + 5);

    return EXIT_SUCCESS;
}

