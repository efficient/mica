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
#include "net_common.h"
#include "proto.h"
#include "stopwatch.h"
#include "netbench_config.h"
#include "netbench_hot_item_hash.h"
#include "table.h"

#include <rte_launch.h>
#include <rte_eal.h>
#include <rte_ethdev.h>
#include <rte_lcore.h>
#include <rte_byteorder.h>
#include <rte_log.h>
#include <rte_malloc.h>
#include <rte_debug.h>

#ifdef MEHCACHED_USE_IB
#include "../ib-dpdk/hrd.h"
#endif

#if !defined(NETBENCH_SERVER_MEMCACHED) && !defined(NETBENCH_SERVER_MASSTREE) && !defined(NETBENCH_SERVER_RAMCLOUD)
#define NETBENCH_SERVER_MEHCACHED
#endif

#ifdef NETBENCH_SERVER_MEMCACHED
#include <memcached_export.h>
#endif
#ifdef NETBENCH_SERVER_MASSTREE
#include <masstree_export.h>
#endif
#ifdef NETBENCH_SERVER_RAMCLOUD
#include <ramcloud_export.h>
#endif

#ifdef MEHCACHED_USE_IB
#define TX_MAX_OUTSTANDING (16 + HRD_SS_WINDOW)
#endif

//#define USE_HOT_ITEMS

//#define USE_PAUSE_BASED_SPINNING
#define USE_BALLOON

struct server_state
{
    struct mehcached_server_conf *server_conf;
    struct mehcached_prepopulation_conf *prepopulation_conf;
#ifdef NETBENCH_SERVER_MEHCACHED
    struct mehcached_table **partitions;
#endif
#ifdef NETBENCH_SERVER_MEMCACHED
    // memcached uses a singleton
#endif
#ifdef NETBENCH_SERVER_MASSTREE
    masstree_t masstree;
#endif
#ifdef NETBENCH_SERVER_RAMCLOUD
    ramcloud_t ramcloud;
#endif

#ifdef USE_HOT_ITEMS
    struct mehcached_hot_item_hash hot_item_hash;
#endif
    uint32_t target_request_rate;   // target request rate (in ops) for each client thread

#ifdef USE_BALLOON
	uint64_t balloon_cycles;			// per-operation delay in cycles to probe the maximum slack
#endif

#ifdef MEHCACHED_USE_IB
	struct hrd_ctrl_blk *cb[MEHCACHED_MAX_PORTS];
	struct hrd_mbuf *tx_pkts[TX_MAX_OUTSTANDING];
	uint16_t next_tx_pkt;
#endif

    int cpu_mode;
    int port_mode;

    // runtime state
    uint64_t num_operations_done;
    uint64_t num_key0_operations_done;
    uint64_t num_operations_succeeded;
    uint64_t num_rx_burst;
    uint64_t num_rx_received;
    uint64_t num_tx_sent;
    uint64_t num_tx_dropped;
    uint64_t bytes_rx;
    uint64_t bytes_tx;
    uint64_t num_per_partition_ops[MEHCACHED_MAX_PARTITIONS];
    uint64_t last_num_operations_done;
    uint64_t last_num_key0_operations_done;
    uint64_t last_num_operations_succeeded;
    uint64_t last_num_rx_burst;
    uint64_t last_num_rx_received;
    uint64_t last_num_tx_sent;
    uint64_t last_num_tx_dropped;
    uint64_t last_bytes_rx;
    uint64_t last_bytes_tx;
    uint64_t last_num_per_partition_ops[MEHCACHED_MAX_PARTITIONS];
    uint16_t packet_size;

#ifdef MEHCACHED_USE_SOFT_FDIR
    // struct rte_ring *soft_fdir_mailbox[MEHCACHED_MAX_THREADS] __rte_cache_aligned;
    struct rte_ring *soft_fdir_mailbox[MEHCACHED_MAX_NUMA_NODES] __rte_cache_aligned;
    uint64_t num_soft_fdir_dropped[MEHCACHED_MAX_PORTS];
#endif
} __rte_cache_aligned;

//#ifdef MEHCACHED_MEASURE_LATENCY
static uint32_t target_request_rate_from_user;
//#endif

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
uint16_t
mehcached_get_partition_id(struct server_state *state, uint64_t key_hash)
{
#ifdef USE_HOT_ITEMS
    uint8_t hot_item_id = mehcached_get_hot_item_id(state->server_conf, &state->hot_item_hash, key_hash);
    if (hot_item_id != (uint8_t)-1)
        return (uint16_t)(state->server_conf->num_partitions + state->server_conf->hot_items[hot_item_id].thread_id);
    else
#endif
        //return (uint16_t)(key_hash >> 48) & (uint16_t)(state->server_conf->num_partitions - 1);
		// for non power-of-two num_partitions
        return (uint16_t)(key_hash >> 48) % (uint16_t)state->server_conf->num_partitions;
}

static
void
#ifndef MEHCACHED_USE_IB
mehcached_remote_send_response(struct server_state *state, struct rte_mbuf *mbuf, uint8_t port_id)
#else
mehcached_remote_send_response(struct server_state *state, struct hrd_mbuf *mbuf, uint8_t port_id)
#endif
{
#ifndef MEHCACHED_USE_IB
    struct mehcached_batch_packet *packet = rte_pktmbuf_mtod(mbuf, struct mehcached_batch_packet *);
#else
    struct mehcached_batch_packet *packet = (struct mehcached_batch_packet *)hrd_pktmbuf_mtod(mbuf);
	(void)port_id;
#endif

    // update stats
    uint8_t request_index;
    const uint8_t *next_key = packet->data + sizeof(struct mehcached_request) * (size_t)packet->num_requests;
    for (request_index = 0; request_index < packet->num_requests; request_index++)
    {
        struct mehcached_request *req = (struct mehcached_request *)packet->data + request_index;

		// we cannot do this here because responses remove the key and contain values
        // state->num_operations_done++;
        //
		// if (*(const uint64_t *)next_key == 1 && MEHCACHED_KEY_LENGTH(req->kv_length_vec) <= 8)	// for little-endian
        //    state->num_key0_operations_done++;

        if (req->result == MEHCACHED_OK)
            state->num_operations_succeeded++;

        next_key += MEHCACHED_ROUNDUP8(MEHCACHED_KEY_LENGTH(req->kv_length_vec)) + MEHCACHED_ROUNDUP8(MEHCACHED_VALUE_LENGTH(req->kv_length_vec));
    }

#ifndef MEHCACHED_USE_IB
    struct ether_hdr *eth = (struct ether_hdr *)rte_pktmbuf_mtod(mbuf, unsigned char *);
#else
    struct ether_hdr *eth = (struct ether_hdr *)hrd_pktmbuf_mtod(mbuf);
#endif
    struct ipv4_hdr *ip = (struct ipv4_hdr *)((unsigned char *)eth + sizeof(struct ether_hdr));
    struct udp_hdr *udp = (struct udp_hdr *)((unsigned char *)ip + sizeof(struct ipv4_hdr));

    uint16_t packet_length = (uint16_t)(next_key - (uint8_t *)packet);

#ifdef MEHCACHED_ENABLE_THROTTLING
    // server load feedback
    // abuse the opaque field because it is going to be used for flow control anyway if clients can do full RX
    packet->opaque = state->target_request_rate;
#endif

    // TODO: update IP checksum

    // swap source and destination
    {
        struct ether_addr t = eth->s_addr;
        eth->s_addr = eth->d_addr;
        eth->d_addr = t;
    }
    {
        uint32_t t = ip->src_addr;
        ip->src_addr = ip->dst_addr;
        ip->dst_addr = t;
    }
    {
        uint16_t t = udp->src_port;
        udp->src_port = udp->dst_port;
        udp->dst_port = t;
    }

#ifdef MEHCACHED_USE_IB
	mbuf->d_lid = mbuf->s_lid;
	mbuf->d_qpn = mbuf->s_qpn;
#endif

    // reset TTL
    ip->time_to_live = 64;

    ip->total_length = rte_cpu_to_be_16((uint16_t)(packet_length - sizeof(struct ether_hdr)));
    udp->dgram_len = rte_cpu_to_be_16((uint16_t)(packet_length - sizeof(struct ether_hdr) - sizeof(struct ipv4_hdr)));

    mbuf->data_len = (uint16_t)packet_length;
#ifndef MEHCACHED_USE_IB
    mbuf->pkt_len = (uint32_t)packet_length;
    mbuf->next = NULL;
    mbuf->nb_segs = 1;
    mbuf->ol_flags = 0;
#endif

#ifndef NDEBUG
#ifndef MEHCACHED_USE_IB
    rte_mbuf_sanity_check(mbuf, 1);
    if (rte_pktmbuf_headroom(mbuf) + mbuf->data_len > mbuf->buf_len)
    {
        printf("data_len = %hd\n", mbuf->data_len);
        uint8_t request_index;
        const uint8_t *next_key = packet->data + sizeof(struct mehcached_request) * (size_t)packet->num_requests;
        for (request_index = 0; request_index < packet->num_requests; request_index++)
        {
            struct mehcached_request *req = (struct mehcached_request *)packet->data + request_index;

            printf("%hhu: %hhu %hhu %u %u\n", request_index, req->operation, req->result, MEHCACHED_KEY_LENGTH(req->kv_length_vec), MEHCACHED_VALUE_LENGTH(req->kv_length_vec));
            next_key += MEHCACHED_ROUNDUP8(MEHCACHED_KEY_LENGTH(req->kv_length_vec)) + MEHCACHED_ROUNDUP8(MEHCACHED_VALUE_LENGTH(req->kv_length_vec));
        }
    }
    assert(rte_pktmbuf_headroom(mbuf) + mbuf->data_len <= mbuf->buf_len);
#else
	assert(mbuf->data_len <= HRD_MAX_DATA);
#endif
#endif

#ifndef MEHCACHED_USE_IB
    mehcached_send_packet(port_id, mbuf);
#else
	assert(state->next_tx_pkt < TX_MAX_OUTSTANDING);
	state->tx_pkts[state->next_tx_pkt++] = mbuf;
#endif

    state->bytes_tx += (uint64_t)(packet_length + 24);  // 24 for PHY overheads
}

static
int
mehcached_benchmark_consume_packets_proc(void *arg)
{
    uint8_t num_ports = (uint8_t)(size_t)arg;

    // discard some initial packets that may have been misclassified (there is a small window before setting perfect filters)
    // 4096 is the maximum number of descriptors in a ring
    int i;
    uint8_t port_id;
    for (port_id = 0; port_id < num_ports; port_id++)
    {
        for (i = 0; i < 4096; i++)
        {
            struct rte_mbuf *mbuf = mehcached_receive_packet(port_id);
            if (mbuf != NULL)
                mehcached_packet_free(mbuf);
            else
                break;
        }
    }
    return 0;
}

static
int
mehcached_benchmark_server_proc(void *arg)
{
    struct server_state **states = (struct server_state **)arg;

    uint8_t thread_id = (uint8_t)rte_lcore_id();
    struct server_state *state = states[thread_id];
    struct mehcached_server_conf *server_conf = state->server_conf;
    struct mehcached_server_thread_conf *thread_conf = &server_conf->threads[thread_id];

#ifdef USE_HOT_ITEMS
    mehcached_calc_hot_item_hash(state->server_conf, &state->hot_item_hash);
#endif

    // for single core performance test
    // if (thread_id != 0 && thread_id != 1)
    //     return 0;

#ifdef MEHCACHED_USE_IB
	// see netbench_client.c:mehcached_benchmark_client_proc() for the reason why we do this here
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
			hrd_register_qp(state->cb[port_id], addr);
		}
	}

	ib_init_thread_id++;
#endif

    if (thread_id != thread_id % (server_conf->num_threads >> state->cpu_mode))
        return 0;

    uint64_t t_start;
    uint64_t t_end;
    double diff;
    double prev_report = 0.;
    double prev_rate_update = 0.;
    uint64_t t_last_rx[MEHCACHED_MAX_PORTS];
    uint64_t t_last_tx_flush[MEHCACHED_MAX_PORTS];

    t_start = mehcached_stopwatch_now();

    {
        uint8_t port_index;
        for (port_index = 0; port_index < thread_conf->num_ports; port_index++)
        {
            t_last_rx[port_index] = t_start;
            t_last_tx_flush[port_index] = t_start;
        }
    }

    uint64_t i = 0;

    uint64_t last_ipackets[MEHCACHED_MAX_PORTS];
    uint64_t last_ierrors[MEHCACHED_MAX_PORTS];
    uint64_t last_opackets[MEHCACHED_MAX_PORTS];
    uint64_t last_oerrors[MEHCACHED_MAX_PORTS];

    if (thread_id == 0)
    {
        uint8_t port_id;
        for (port_id = 0; port_id < server_conf->num_ports; port_id++)
        {
            struct rte_eth_stats stats;
            rte_eth_stats_get(port_id, &stats);

            last_ipackets[port_id] = stats.ipackets;
            last_ierrors[port_id] = stats.ierrors;
            // last_opackets[port_id] = stats.opackets;
            // last_oerrors[port_id] = stats.oerrors;

            uint64_t opackets = 0;
            uint64_t oerrors = 0;
            uint8_t thread_id;
            for (thread_id = 0; thread_id < server_conf->num_threads; thread_id++)
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

#ifndef MEHCACHED_USE_IB
    const size_t pipeline_size = 32;
    const size_t max_pending_packets = 32;
#else
    const size_t pipeline_size = 16;
    const size_t max_pending_packets = 16;
	assert(pipeline_size + HRD_SS_WINDOW <= TX_MAX_OUTSTANDING);
#endif

#define USE_STAGE_GAP

#ifndef USE_STAGE_GAP
    const uint64_t min_delay_stage0 = 100;
    const uint64_t min_delay_stage1 = 100;
    const uint64_t min_delay_stage2 = 100;
#else
    const size_t stage_gap = 2;
#endif

    size_t next_port_index = 0;

    while (!exiting)
    {
#ifndef USE_STAGE_GAP
        uint64_t prefetch_time[pipeline_size];
#endif

        // invariant: 0 <= stage3_index <= stage2_index <= stage1_index <= stage0_index <= packet_count <= pipeline_size
        size_t packet_count = 0;
        size_t stage0_index = 0;
        size_t stage1_index = 0;
        size_t stage2_index = 0;
        size_t stage3_index = 0;

        // RX
#ifndef MEHCACHED_USE_IB
        struct rte_mbuf *packet_mbufs[pipeline_size];
#else
        struct hrd_mbuf *packet_mbufs[pipeline_size];
#endif
        // stage0
        struct mehcached_batch_packet *packets[pipeline_size];
        // stage1
#ifdef NETBENCH_SERVER_MEHCACHED
#ifdef USE_HOT_ITEMS
        uint16_t partition_ids[pipeline_size];
#endif
#endif
        // stage1 & stage2
#ifdef NETBENCH_SERVER_MEHCACHED
        struct mehcached_prefetch_state prefetch_state[pipeline_size][MEHCACHED_MAX_BATCH_SIZE];
#endif
        uint8_t port_id = thread_conf->port_ids[next_port_index];

        // while (true)
        // {
        //     struct rte_mbuf *mbuf = mehcached_receive_packet(port_id);
        //     if (mbuf == NULL)
        //         break;

        //     state->bytes_rx += (uint64_t)(mbuf->data_len + 24);   // 24 for PHY overheads

        //     packet_mbufs[packet_count] = mbuf;
        //     packet_count++;
        //     if (packet_count == pipeline_size)
        //         break;
        // }

        t_end = mehcached_stopwatch_now();

        // receive packets
        // the minimum retrieval interval of 1 us avoids excessive PCIe use, which causes slowdowns in skewed workloads
        // (most cores cause small batches, which reduces available bandwidth for the loaded cores)

		if (thread_conf->num_ports == 0) {
#ifdef USE_PAUSE_BASED_SPINNING
			// try to reduce the load of spinning
			rte_delay_ms(1);
#endif

			// no RX
			packet_count = 0;
		}
        else if (t_end - t_last_rx[next_port_index] >= 1 * mehcached_stopwatch_1_usec)
        {
            packet_count = pipeline_size;
#ifndef MEHCACHED_USE_IB
            mehcached_receive_packets(port_id, packet_mbufs, &packet_count);
#else
			int recv_comps = hrd_rx_burst(state->cb[port_id], packet_mbufs, (uint16_t)pipeline_size);
			packet_count = (size_t)recv_comps;
			// if (recv_comps > 0)
			// {
			// 	printf("recv_comps %d\n", recv_comps);
			// 	printf("packet 0 len %d\n", packet_mbufs[0]->data_len);
			// }
#endif
            t_last_rx[next_port_index] = t_end;
        }
        else {
            packet_count = 0;

#ifdef USE_PAUSE_BASED_SPINNING
			// try to reduce the load of spinning
			rte_pause();
#endif
		}

#ifdef MEHCACHED_USE_SOFT_FDIR
        {
            // enqueue to soft_fdir_mailbox
            size_t packet_index;
            for (packet_index = 0; packet_index < packet_count; packet_index++)
            {
                struct rte_mbuf *mbuf = packet_mbufs[packet_index];
                struct mehcached_batch_packet *packet = rte_pktmbuf_mtod(mbuf, struct mehcached_batch_packet *);
                struct mehcached_request *req = (struct mehcached_request *)packet->data + 0;
                uint64_t key_hash = req->key_hash;
                uint16_t partition_id = mehcached_get_partition_id(state, key_hash);
                uint8_t owner_thread_id = server_conf->partitions[partition_id].thread_id;
                uint8_t target_thread_id;
                // XXX: this does not support hot items
                if (req->operation == MEHCACHED_NOOP_READ || req->operation == MEHCACHED_GET)
                {
                    if (MEHCACHED_CONCURRENT_TABLE_READ(server_conf, partition_id))
                        target_thread_id = thread_id;
                    else
                        target_thread_id = owner_thread_id;
                }
                else
                {
                    if (MEHCACHED_CONCURRENT_TABLE_WRITE(server_conf, partition_id))
                        target_thread_id = thread_id;
                    else
                        target_thread_id = owner_thread_id;
                }

                // uint8_t mailbox_index = thread_id;
                uint8_t mailbox_index = (uint8_t)rte_lcore_to_socket_id(thread_id);
                // if (rte_ring_sp_enqueue(states[target_thread_id]->soft_fdir_mailbox[mailbox_index], mbuf) == -ENOBUFS)
                if (rte_ring_mp_enqueue(states[target_thread_id]->soft_fdir_mailbox[mailbox_index], mbuf) == -ENOBUFS)
                {
                    state->num_soft_fdir_dropped[port_id]++;
                    mehcached_packet_free(mbuf);
                }
            }
            // dequeue from soft_fdir_mailbox
            packet_count = 0;
            size_t mailbox_index;
            // for (mailbox_index = 0; mailbox_index < MEHCACHED_MAX_THREADS; mailbox_index++)
            for (mailbox_index = 0; mailbox_index < MEHCACHED_MAX_NUMA_NODES; mailbox_index++)
                packet_count += (size_t)rte_ring_sc_dequeue_burst(state->soft_fdir_mailbox[mailbox_index], (void **)(packet_mbufs + packet_count), (unsigned int)(pipeline_size - packet_count));
        }
#endif

        // update RX byte statistics
        {
            size_t packet_index;
            for (packet_index = 0; packet_index < packet_count; packet_index++)
                state->bytes_rx += (uint64_t)(packet_mbufs[packet_index]->data_len + 24);   // 24 for PHY overheads
        }


#ifndef USE_STAGE_GAP
        uint64_t t = mehcached_stopwatch_now();
#endif

#ifdef USE_BALLOON
		size_t num_requests_in_this_batch = 0;
#endif

        while (stage3_index < packet_count)
        {
#ifndef USE_STAGE_GAP
            if (stage0_index < packet_count && stage0_index - stage3_index < max_pending_packets)
#else
            if (stage0_index < packet_count && stage0_index - stage3_index < max_pending_packets && stage0_index - stage1_index < stage_gap)
#endif
            {
#ifndef MEHCACHED_USE_IB
                struct rte_mbuf *mbuf = packet_mbufs[stage0_index];
                struct mehcached_batch_packet *packet = packets[stage0_index] = rte_pktmbuf_mtod(mbuf, struct mehcached_batch_packet *);
#else
                struct hrd_mbuf *mbuf = packet_mbufs[stage0_index];
                struct mehcached_batch_packet *packet = packets[stage0_index] = (struct mehcached_batch_packet *)hrd_pktmbuf_mtod(mbuf);
#endif
                __builtin_prefetch(packet, 0, 0);
                __builtin_prefetch(&packet->data, 0, 0);
#ifndef USE_STAGE_GAP
                prefetch_time[stage0_index] = t;
#endif
                stage0_index++;
            }
#ifndef USE_STAGE_GAP
            else if (stage1_index < stage0_index && t - prefetch_time[stage1_index] >= min_delay_stage0)
#else
            else if (stage1_index < stage0_index && stage1_index - stage2_index < stage_gap)
#endif
            {
#ifdef NETBENCH_SERVER_MEHCACHED
                struct mehcached_batch_packet *packet = packets[stage1_index];
                assert(packet->num_requests <= MEHCACHED_MAX_BATCH_SIZE);
                uint8_t request_index;
                for (request_index = 0; request_index < packet->num_requests; request_index++)
                {
                    struct mehcached_request *req = (struct mehcached_request *)packet->data + request_index;
                    if (req->operation != MEHCACHED_NOOP_READ && req->operation != MEHCACHED_NOOP_WRITE)
                    {
                        uint64_t key_hash = req->key_hash;
                        uint16_t partition_id;
#ifdef USE_HOT_ITEMS
                        if (request_index == 0)
                            partition_id = partition_ids[stage1_index] = mehcached_get_partition_id(state, key_hash);
                        else
                        {
                            partition_id = partition_ids[stage1_index];
                            assert(partition_id == mehcached_get_partition_id(state, key_hash));
                        }
#else
                        partition_id = mehcached_get_partition_id(state, key_hash);
#endif
                        struct mehcached_table *partition = state->partitions[partition_id];
                        mehcached_prefetch_table(partition, key_hash, &prefetch_state[stage1_index][request_index]);
                    }
#ifdef USE_HOT_ITEMS
                    else
                        partition_ids[stage1_index] = 0;  // any partition can handle no-op requests
#endif
                }
#endif
#ifndef USE_STAGE_GAP
                prefetch_time[stage1_index] = t;
#endif
                stage1_index++;
            }
#ifndef USE_STAGE_GAP
            else if (stage2_index < stage1_index && t - prefetch_time[stage2_index] >= min_delay_stage1)
#else
            else if (stage2_index < stage1_index && stage2_index - stage3_index < stage_gap)
#endif
            {
#ifdef NETBENCH_SERVER_MEHCACHED
                struct mehcached_batch_packet *packet = packets[stage2_index];
                uint8_t request_index;
                for (request_index = 0; request_index < packet->num_requests; request_index++)
                {
                    struct mehcached_request *req = (struct mehcached_request *)packet->data + request_index;
                    if (req->operation != MEHCACHED_NOOP_READ && req->operation != MEHCACHED_NOOP_WRITE)
                        mehcached_prefetch_alloc(&prefetch_state[stage2_index][request_index]);
                }
#endif
#ifndef USE_STAGE_GAP
                prefetch_time[stage2_index] = t;
#endif
                stage2_index++;
            }
#ifndef USE_STAGE_GAP
            else if (stage3_index < stage2_index && t - prefetch_time[stage3_index] >= min_delay_stage2)
#else
            else if (stage3_index < stage2_index)
#endif
            {
#ifndef MEHCACHED_USE_IB
                struct rte_mbuf *mbuf = packet_mbufs[stage3_index];
#else
                struct hrd_mbuf *mbuf = packet_mbufs[stage3_index];
#endif
                struct mehcached_batch_packet *packet = packets[stage3_index];
#ifdef NETBENCH_SERVER_MEHCACHED
#ifdef USE_HOT_ITEMS
                uint16_t partition_id = partition_ids[stage3_index];
#else
                struct mehcached_request *req = (struct mehcached_request *)packet->data + 0;
                uint16_t partition_id = mehcached_get_partition_id(state, req->key_hash);
#endif
#endif

                uint8_t new_key_values[ETHER_MAX_LEN - ETHER_CRC_LEN - sizeof(struct mehcached_batch_packet)];
                size_t new_key_value_length = ETHER_MAX_LEN - ETHER_CRC_LEN - sizeof(struct mehcached_batch_packet) - sizeof(struct mehcached_request) * (size_t)packet->num_requests;

#ifdef MEHCACHED_MEASURE_LATENCY
                uint32_t org_expire_time;
                {
                    struct mehcached_request *req = (struct mehcached_request *)packet->data + 0;
                    org_expire_time = req->expire_time;
                }
#endif

#ifdef NETBENCH_SERVER_MEHCACHED
                struct mehcached_table *partition = state->partitions[partition_id];
                //struct mehcached_request *requests = (struct mehcached_request *)packet->data;

                uint8_t alloc_id;
                if (partition_id < state->server_conf->num_partitions)
                    // alloc_id = (uint8_t)((MEHCACHED_CONCURRENT_TABLE_WRITE(state->server_conf, partition_id) && !MEHCACHED_CONCURRENT_ALLOC_WRITE(state->server_conf, partition_id)) ? (rte_lcore_id() >> 1) : 0);
                    alloc_id = (uint8_t)((MEHCACHED_CONCURRENT_TABLE_WRITE(state->server_conf, partition_id) && !MEHCACHED_CONCURRENT_ALLOC_WRITE(state->server_conf, partition_id)) ? rte_lcore_id() : 0);
                else
                    alloc_id = 0;

                bool readonly;
                if (thread_id == state->server_conf->partitions[partition_id].thread_id)
                    readonly = false;
                else if (MEHCACHED_CONCURRENT_TABLE_WRITE(state->server_conf, partition_id))
                    readonly = false;
                else
                    readonly = true;

#ifdef MEHCACHED_COLLECT_PER_PARTITION_LOAD
                state->num_per_partition_ops[partition_id] += packet->num_requests;
#endif

#ifdef USE_BALLOON
				num_requests_in_this_batch += packet->num_requests;
#endif
                //mehcached_process_batch(alloc_id, partition, requests, packet->num_requests, packet->data + sizeof(struct mehcached_request) * (size_t)packet->num_requests, new_key_values, &new_key_value_length, readonly);
#endif
//#if defined(NETBENCH_SERVER_MEMCACHED) || defined(NETBENCH_SERVER_MASSTREE)
                uint8_t *out_data_p = new_key_values;
                const uint8_t *out_data_end = out_data_p + new_key_value_length;

                uint8_t request_index;
                const uint8_t *in_data_p = packet->data + sizeof(struct mehcached_request) * (size_t)packet->num_requests;
                for (request_index = 0; request_index < packet->num_requests; request_index++)
                {
                    struct mehcached_request *req = (struct mehcached_request *)packet->data + request_index;
                    size_t key_length = MEHCACHED_KEY_LENGTH(req->kv_length_vec);
                    size_t value_length = MEHCACHED_VALUE_LENGTH(req->kv_length_vec);
                    const uint8_t *key = in_data_p;
                    const uint8_t *value = in_data_p + MEHCACHED_ROUNDUP8(key_length);
                    //const uint8_t *key = (const uint8_t *)&req->key_hash;
                    //key_length = sizeof(req->key_hash);
                    in_data_p += MEHCACHED_ROUNDUP8(key_length) + MEHCACHED_ROUNDUP8(value_length);

					state->num_operations_done++;
					if (*(const uint64_t *)key == 1 && key_length <= 8)	// for little-endian
						state->num_key0_operations_done++;

                    switch (req->operation)
                    {
                        case MEHCACHED_NOOP_READ:
                        case MEHCACHED_NOOP_WRITE:
                            {
                                req->result = MEHCACHED_OK;
                                req->kv_length_vec = 0;
                            }
                            break;
                        case MEHCACHED_ADD:
                        case MEHCACHED_SET:
                            {
                                if (0)
                                {
                                    // for debug: concurrent read/write validation
                                    uint64_t v = *(uint64_t *)value;
                                    if ((v & 0xffffffff) != ((~v >> 32) & 0xffffffff))
                                    {
                                        static unsigned int p = 0;
#ifndef NETBENCH_SERVER_MEHCACHED
                                        uint16_t partition_id = 0;
#endif
                                        if ((p++ & 63) == 0)
                                            fprintf(stderr, "thread %hhu partition %hu unexpected value being written: %lu %lu\n", thread_id, partition_id, (v & 0xffffffff), ((~v >> 32) & 0xffffffff));
                                    }
                                }
#ifdef NETBENCH_SERVER_MEHCACHED
                                if (mehcached_set(alloc_id, partition, req->key_hash, key, key_length, value, value_length, req->expire_time, req->operation == MEHCACHED_SET))
#endif
#ifdef NETBENCH_SERVER_MEMCACHED
                                if (process_update((char *)key, key_length, (char *)value, value_length, false, true, false))
#endif
#ifdef NETBENCH_SERVER_MASSTREE
                                if (masstree_put(state->masstree, thread_id, (const char *)key, key_length, (const char *)value, value_length))
#endif
#ifdef NETBENCH_SERVER_RAMCLOUD
                                if (ramcloud_put(state->ramcloud, thread_id, (const char *)key, key_length, (const char *)value, value_length))
#endif
                                    req->result = MEHCACHED_OK;
                                else
                                    req->result = MEHCACHED_ERROR;
                                req->kv_length_vec = 0;
                            }
                            break;
                        case MEHCACHED_GET:
                            {
                                size_t out_value_length = (size_t)(out_data_end - out_data_p);
                                uint8_t *out_value = out_data_p;
#ifdef NETBENCH_SERVER_MEHCACHED
                                if (mehcached_get(alloc_id, partition, req->key_hash, key, key_length, out_value, &out_value_length, &req->expire_time, readonly))
#endif
#ifdef NETBENCH_SERVER_MEMCACHED
                                if (process_get((char *)key, key_length, (char *)out_value, &out_value_length))
#endif
#ifdef NETBENCH_SERVER_MASSTREE
                                if (masstree_get(state->masstree, thread_id, (const char *)key, key_length, (char *)out_value, &out_value_length))
#endif
#ifdef NETBENCH_SERVER_RAMCLOUD
                                if (ramcloud_get(state->ramcloud, thread_id, (const char *)key, key_length, (char *)out_value, &out_value_length))
#endif
                                {
                                    req->result = MEHCACHED_OK;

                                    if (0)
                                    {
                                        // for debug: concurrent read/write validation
                                        uint64_t v = *(uint64_t *)out_value;
                                        if ((v & 0xffffffff) != ((~v >> 32) & 0xffffffff))
                                        {
                                            static unsigned int p = 0;
    #ifndef NETBENCH_SERVER_MEHCACHED
                                            uint16_t partition_id = 0;
    #endif
                                            if ((p++ & 63) == 0)
                                                fprintf(stderr, "thread %hhu partition %hu unexpected value being read: %lu %lu\n", thread_id, partition_id, (v & 0xffffffff), ((~v >> 32) & 0xffffffff));
                                        }
                                    }
                                }
                                else
                                {
                                    req->result = MEHCACHED_ERROR;  // TODO: return a correct failure code
                                    out_value_length = 0;
                                }
                                req->kv_length_vec = MEHCACHED_KV_LENGTH_VEC(0, out_value_length);
#ifndef NETBENCH_SERVER_MEHCACHED
                                req->expire_time = 0;
#endif
                                out_data_p += MEHCACHED_ROUNDUP8(out_value_length);
                            }
                            break;
                        case MEHCACHED_INCREMENT:
                            {
                                // TODO: check if the output space is large enough
                                // TODO: use expire_time
                                uint64_t increment;
                                assert(value_length == sizeof(uint64_t));
                                mehcached_memcpy8((uint8_t *)&increment, value, sizeof(uint64_t));
                                size_t out_value_length = sizeof(uint64_t);
                                uint8_t *out_value = out_data_p;
#ifdef NETBENCH_SERVER_MEHCACHED
                                if (mehcached_increment(alloc_id, partition, req->key_hash, key, key_length, increment, (uint64_t *)out_value, req->expire_time))
#endif
#ifdef NETBENCH_SERVER_MEMCACHED
                                if (process_add_delta((char *)key, key_length, increment, (uint64_t *)out_value))
#endif
#ifdef NETBENCH_SERVER_MASSTREE
                                (void)out_value;
                                if (0)  // masstree does not have native support for atomic increment operations
#endif
#ifdef NETBENCH_SERVER_RAMCLOUD
                                (void)out_value;
                                if (0)  // ramcloud does not have native support for atomic increment operations
#endif
                                    req->result = MEHCACHED_OK;
                                else
                                {
                                    req->result = MEHCACHED_ERROR;  // TODO: return a correct failure code
                                    out_value_length = 0;
                                }
                                req->kv_length_vec = MEHCACHED_KV_LENGTH_VEC(0, out_value_length);
                                out_data_p += MEHCACHED_ROUNDUP8(out_value_length);
                            }
                            break;
                        default:
                            fprintf(stderr, "invalid operation or not implemented\n");
                            break;
                    }
                }
                new_key_value_length = (size_t)(out_data_p - new_key_values);
//#endif

                rte_memcpy(packet->data + sizeof(struct mehcached_request) * (size_t)packet->num_requests, new_key_values, new_key_value_length);
                // new packet length will be calculated in mehcached_remote_send_response()

#ifdef MEHCACHED_MEASURE_LATENCY
                {
                    struct mehcached_request *req = (struct mehcached_request *)packet->data + 0;
                    req->expire_time = org_expire_time;
                }
#endif

                mehcached_remote_send_response(state, mbuf, port_id);

                stage3_index++;
            }
#ifndef USE_STAGE_GAP
            else
                t = (uint32_t)mehcached_stopwatch_now();
#endif
        }

#ifdef MEHCACHED_USE_IB
		// all pending TX packets must be flushed because tx_pkts and next_tx_pkt are shared
		if (state->next_tx_pkt > 0)
		{
			// printf("next_tx_pkt %hu\n", state->next_tx_pkt);
			// printf("packet 0 d_lid %d d_qpn %d\n", state->tx_pkts[0]->d_lid, state->tx_pkts[0]->d_qpn);
			hrd_tx_burst(state->cb[port_id], state->tx_pkts, state->next_tx_pkt);
			state->next_tx_pkt = 0;
		}
#endif

        t_end = mehcached_stopwatch_now();

		if (thread_conf->num_ports == 0) {
			// no TX flush
		}
		else
        //if (packet_count < pipeline_size)
        {
            // send out packets (improves latency under low throughput)
            // the minimum flush interval of 10 us avoids excessive PCIe use, which causes slowdowns in skewed workloads
            // (most cores cause small batches, which reduces available bandwidth for the loaded cores)
            // in other words, no TX packets will stay in the application-level queue much longer than 10 us
            if (t_end - t_last_tx_flush[next_port_index] >= 10 * mehcached_stopwatch_1_usec)
            {
                t_last_tx_flush[next_port_index] = t_end;
#ifndef MEHCACHED_USE_IB
                mehcached_send_packet_flush(port_id);
#endif
            }

            next_port_index += (uint8_t)(1 << state->port_mode);
            if (next_port_index >= thread_conf->num_ports)
                next_port_index = 0;
        }

#ifdef USE_BALLOON
		uint64_t wait_until = t_end + state->balloon_cycles * num_requests_in_this_batch;
		while ((int64_t)(wait_until - t_end) > 0)
			t_end = mehcached_stopwatch_now();
#endif

        i++;

//        if ((i & 0xff) == 0)
        if ((i % 20000) == 0)
        {
            uint64_t total_num_rx_burst = 0;
            uint64_t total_num_rx_received = 0;
            uint64_t total_num_tx_sent = 0;
            uint64_t total_num_tx_dropped = 0;
            size_t port_index;
            for (port_index = 0; port_index < thread_conf->num_ports; port_index++)
            {
                uint8_t port_id = thread_conf->port_ids[port_index];
                uint64_t num_rx_burst;
                uint64_t num_rx_received;
                uint64_t num_tx_sent;
                uint64_t num_tx_dropped;
#ifndef MEHCACHED_USE_IB
                mehcached_get_stats(port_id, &num_rx_burst, &num_rx_received, NULL, &num_tx_sent, &num_tx_dropped);
#else
				(void)port_id;
				num_rx_burst = 0;
				num_rx_received = 0;
				num_tx_sent = 0;
				num_tx_dropped = 0;
#endif
                total_num_rx_burst += num_rx_burst;
                total_num_rx_received += num_rx_received;
                total_num_tx_sent += num_tx_sent;
                total_num_tx_dropped += num_tx_dropped;
            }
            state->num_rx_burst = total_num_rx_burst;
            state->num_rx_received = total_num_rx_received;
            state->num_tx_sent = total_num_tx_sent;
            state->num_tx_dropped = total_num_tx_dropped;

            //t_end = mehcached_stopwatch_now();
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
                    for (thread_id = 0; thread_id < server_conf->num_threads; thread_id++)
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
                            success_rate = 1.;  // total_new_num_operations_succeeded & total_new_num_operations_done are measured for a different set of requests
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

                    printf("; target_request_rate: %.3f Mops (s)", (float)state->target_request_rate * 0.000001f);

                    printf("; avg_rx_burst");
                    {
                        uint64_t total_num_rx_burst = 0;
                        uint64_t total_num_rx_received = 0;
                        for (thread_id = 0; thread_id < server_conf->num_threads; thread_id++)
                        {
                            uint64_t num_rx_burst = states[thread_id]->num_rx_burst;
                            uint64_t new_num_rx_burst = num_rx_burst - states[thread_id]->last_num_rx_burst;
                            states[thread_id]->last_num_rx_burst = num_rx_burst;

                            uint64_t num_rx_received = states[thread_id]->num_rx_received;
                            uint64_t new_num_rx_received = num_rx_received - states[thread_id]->last_num_rx_received;
                            states[thread_id]->last_num_rx_received = num_rx_received;

                            double avg_rx_burst_size = 0.;
                            if (new_num_rx_burst != 0)
                                avg_rx_burst_size = (double)new_num_rx_received / (double)new_num_rx_burst;

                            total_num_rx_burst += new_num_rx_burst;
                            total_num_rx_received += new_num_rx_received;

                            printf(" %zu[%.3f]", thread_id, avg_rx_burst_size);
                        }
                        {
                            double avg_rx_burst_size = 0.;
                            if (total_num_rx_burst != 0)
                                avg_rx_burst_size = (double)total_num_rx_received / (double)total_num_rx_burst;
                            printf(" avg[%.3f]", avg_rx_burst_size);
                        }
                    }

#ifdef MEHCACHED_COLLECT_PER_PARTITION_LOAD
                    // calculate per-partition load
                    printf("; load");
                    uint64_t total_num_per_partition_ops[server_conf->num_partitions];
                    memset(total_num_per_partition_ops, 0, sizeof(total_num_per_partition_ops));
                    uint64_t max_ops = 1;

                    size_t partition_id;
                    for (thread_id = 0; thread_id < server_conf->num_threads; thread_id++)
                    {
                        for (partition_id = 0; partition_id < server_conf->num_partitions; partition_id++)
                        {
                            uint64_t num_per_partition_ops = states[thread_id]->num_per_partition_ops[partition_id];
                            uint64_t new_num_per_partition_ops = num_per_partition_ops - states[thread_id]->last_num_per_partition_ops[partition_id];
                            states[thread_id]->last_num_per_partition_ops[partition_id] = num_per_partition_ops;
                            total_num_per_partition_ops[partition_id] += new_num_per_partition_ops;
                        }
                    }
                    for (partition_id = 0; partition_id < server_conf->num_partitions; partition_id++)
                        if (max_ops < total_num_per_partition_ops[partition_id])
                            max_ops = total_num_per_partition_ops[partition_id];
                    for (partition_id = 0; partition_id < server_conf->num_partitions; partition_id++)
                        printf(" %zu[%.3f]", partition_id, (double)total_num_per_partition_ops[partition_id] / (double)max_ops);
#endif

#ifdef MEHCACHED_COLLECT_STATS
                    uint16_t partition_id;
                    for (partition_id = 0; partition_id < server_conf->num_partitions + server_conf->num_threads; partition_id++)
                        mehcached_print_stats(state->partitions[partition_id]);
#endif

                    printf("\n");
                    fflush(stdout);

#ifdef USE_BALLOON
					FILE *fp_balloon = fopen("balloon", "r");
					if (fp_balloon)
					{
						char buf[1024];
						if (fgets(buf, sizeof(buf), fp_balloon) != NULL)
						{
							uint64_t balloon_cycles = (uint64_t)atoll(buf);
							printf("using new balloon: %lu cycles\n", balloon_cycles);

							uint8_t thread_id;
							for (thread_id = 0; thread_id < server_conf->num_threads; thread_id++)
								states[thread_id]->balloon_cycles = balloon_cycles;
						}

						fclose(fp_balloon);
						unlink("balloon");
					}
#endif
                }

                prev_report = diff;
            }

            if (diff - prev_rate_update >= 1.0)
            {
                if (thread_id == 0)
                {
                    float max_loss = 0.;

                    uint8_t port_id;
                    //uint8_t loss_port_id = 0;
                    for (port_id = 0; port_id < server_conf->num_ports; port_id++)
                    {
                        struct rte_eth_stats stats;
                        rte_eth_stats_get(port_id, &stats);

#ifndef MEHCACHED_USE_SOFT_FDIR
                        uint64_t new_ipackets = stats.ipackets - last_ipackets[port_id];
                        last_ipackets[port_id] = stats.ipackets;
                        uint64_t new_ierrors = stats.ierrors - last_ierrors[port_id];
                        last_ierrors[port_id] = stats.ierrors;
#else
                        uint64_t ipackets = stats.ipackets;
                        uint64_t ierrors = stats.ierrors;
                        // treat dropped packets by soft fdir as dropped packets by NIC RX
                        {
                            uint8_t thread_id;
                            for (thread_id = 0; thread_id < server_conf->num_threads; thread_id++)
                            {
                                uint64_t num_soft_fdir_dropped = states[thread_id]->num_soft_fdir_dropped[port_id];
                                ipackets -= num_soft_fdir_dropped;
                                ierrors += num_soft_fdir_dropped;
                            }
                        }
                        uint64_t new_ipackets = ipackets - last_ipackets[port_id];
                        last_ipackets[port_id] = ipackets;
                        uint64_t new_ierrors = ierrors - last_ierrors[port_id];
                        last_ierrors[port_id] = ierrors;
#endif
                        // uint64_t new_opackets = stats.opackets - last_opackets[port_id];
                        // last_opackets[port_id] = stats.opackets;
                        // uint64_t new_oerrors = stats.oerrors - last_oerrors[port_id];
                        // last_oerrors[port_id] = stats.oerrors;

                        float iloss;
                        // float oloss;
                        if (new_ipackets + new_ierrors != 0)
                            iloss = (float)new_ierrors / (float)(new_ipackets + new_ierrors);
                        else
                            iloss = 0.f;
                        // if (new_opackets != 0)
                        //     oloss = (float)new_oerrors / (float)new_opackets;
                        // else
                        //     oloss = 0.f;

                        if (max_loss < iloss)
                        {
                            max_loss = iloss;
                            // loss_port_id = port_id;
                        }
                        // if (max_loss < oloss)
                        //     max_loss = oloss;

#ifndef NDEBUG
                        if (stats.fdirmiss != 0)
                            printf("non-zero fdirmiss on port %hhu\n", port_id);
                        if (stats.rx_nombuf != 0)
                            printf("non-zero rx_nombuf on port %hhu\n", port_id);
#endif
                        // printf(" port %hhu new_ipackets %lu new_ierrors %lu", port_id, new_ipackets, new_ierrors);
                    }
                    // printf(" max_loss i %f", max_loss);
                    // if (max_loss != 0)
                    //     printf("iloss port %hhu\n", loss_port_id);

                    for (port_id = 0; port_id < server_conf->num_ports; port_id++)
                    {
                        uint64_t opackets = 0;
                        uint64_t oerrors = 0;
                        uint8_t thread_id;
                        for (thread_id = 0; thread_id < server_conf->num_threads; thread_id++)
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
                    // printf(" max_loss io %f", max_loss);

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

//#ifdef MEHCACHED_MEASURE_LATENCY
                    if (target_request_rate_from_user > 0)
                        new_target_request_rate = target_request_rate_from_user;
//#endif

                    uint8_t thread_id;
                    for (thread_id = 0; thread_id < server_conf->num_threads; thread_id++)
                        states[thread_id]->target_request_rate = new_target_request_rate;
                }

                prev_rate_update = diff;
            }
        }
    }

    return 0;
}

struct mehcached_diagnosis_arg
{
    uint8_t port_id;
    struct rte_mbuf *ret_mbuf;
};
static
int
mehcached_diagnosis_receive_packet_proc(void *arg)
{
    struct mehcached_diagnosis_arg *diagnosis_arg = (struct mehcached_diagnosis_arg *)arg;
    diagnosis_arg->ret_mbuf = mehcached_receive_packet(diagnosis_arg->port_id);
    return 0;
}

static
void
mehcached_diagnosis(struct mehcached_server_conf *server_conf)
{
    size_t noerror_count = 0;
    size_t error_count = 0;
    uint64_t t_start = mehcached_stopwatch_now();
    uint64_t t_end;

    while (!exiting)
    {
        // diagnosis for wrong mapping
        uint8_t port_id;
        uint8_t thread_id;
        for (port_id = 0; port_id < server_conf->num_ports; port_id++)
        {
            for (thread_id = 0; thread_id < server_conf->num_threads; thread_id++)
            {
                struct mehcached_diagnosis_arg diagnosis_arg;
                diagnosis_arg.port_id = port_id;
                rte_eal_launch(mehcached_diagnosis_receive_packet_proc, &diagnosis_arg, (unsigned int)thread_id);
                rte_eal_mp_wait_lcore();

                struct rte_mbuf *mbuf = diagnosis_arg.ret_mbuf;
                if (mbuf == NULL)
                    continue;

                struct mehcached_batch_packet *packet = rte_pktmbuf_mtod(mbuf, struct mehcached_batch_packet *);

                struct ether_hdr *eth = (struct ether_hdr *)rte_pktmbuf_mtod(mbuf, unsigned char *);
                struct ipv4_hdr *ip = (struct ipv4_hdr *)((unsigned char *)eth + sizeof(struct ether_hdr));
                struct udp_hdr *udp = (struct udp_hdr *)((unsigned char *)ip + sizeof(struct ipv4_hdr));

                uint16_t mapping_id = rte_be_to_cpu_16(udp->dst_port);

                bool correct_port = true;
                bool correct_queue = true;
                if (mapping_id < 1024)
                {
                    uint8_t request_index;
                    for (request_index = 0; request_index < packet->num_requests; request_index++)
                    {
                        struct mehcached_request *req = (struct mehcached_request *)packet->data + request_index;

                        uint64_t key_hash = req->key_hash;
                        //uint16_t partition_id = (uint16_t)(key_hash >> 48) & (uint16_t)(server_conf->num_partitions - 1);
						// for non power-of-two num_partitions
                        uint16_t partition_id = (uint16_t)(key_hash >> 48) % (uint16_t)server_conf->num_partitions;
                        uint8_t expected_thread_id = server_conf->partitions[partition_id].thread_id;

                        correct_queue = thread_id == expected_thread_id;

                        correct_port = false;
                        uint8_t port_index;
                        for (port_index = 0; port_index < server_conf->threads[expected_thread_id].num_ports; port_index++)
                        {
                            if (server_conf->threads[expected_thread_id].port_ids[port_index] == port_id)
                            {
                                correct_port = true;
                                break;
                            }
                        }
                    }
                }
                else
                {
                    // TODO: implement
                }

                if (error_count < 16)   // report up to 16 errors per period
                {
                    if (!correct_port)
                        printf("wrong port: mapping = %hu, port = %hhu; fdir.hash = %hu, fdir.id = %hu\n", mapping_id, port_id, mbuf->hash.fdir.hash, mbuf->hash.fdir.id);

                    if (!correct_queue)
                        printf("wrong queue: mapping = %hu, port = %hhu, queue = %hhu; fdir.hash = %hu, fdir.id = %hu\n", mapping_id, port_id, thread_id, mbuf->hash.fdir.hash, mbuf->hash.fdir.id);
                }

                if (correct_port && correct_queue)
                    noerror_count++;
                else
                    error_count++;

                mehcached_packet_free(mbuf);
            }
        }

        t_end = mehcached_stopwatch_now();
        if (mehcached_stopwatch_diff_in_s(t_end, t_start) >= 1.)
        {
            printf("noerror = %zu, error = %zu\n", noerror_count, error_count);
            t_start = t_end;
            noerror_count = 0;
            error_count = 0;
        }
    }
}

// this must be the same as that of netbench_client.c
static
uint64_t
mehcached_hash_key(uint64_t int_key)
{
    return hash((const uint8_t *)&int_key, 8);
}

static
int
mehcached_benchmark_prepopulate_proc(void *arg)
{
    struct server_state **states = (struct server_state **)arg;

    uint8_t thread_id = (uint8_t)rte_lcore_id();
    struct server_state *state = states[thread_id];
    struct mehcached_server_conf *server_conf = state->server_conf;
    struct mehcached_prepopulation_conf *prepopulation_conf = state->prepopulation_conf;
    unsigned int node_id = rte_lcore_to_socket_id(thread_id);

    printf("prepopulation: num_items %lu key_length %zu value_length %zu on core %hhu\n", prepopulation_conf->num_items, prepopulation_conf->key_length, prepopulation_conf->value_length, thread_id);
    fflush(stdout);

    if (prepopulation_conf->num_items == 0)
        return 0;

    uint64_t t_start = mehcached_stopwatch_now();
    uint64_t t_last_report = t_start;

    uint64_t key[MEHCACHED_ROUNDUP8(prepopulation_conf->key_length) / 8 + 1];
    uint64_t value[MEHCACHED_ROUNDUP8(prepopulation_conf->value_length) / 8 + 1];
    memset(key, 0, sizeof(key));
    memset(value, 0, sizeof(value));

    size_t log16_num_items = 0;
    while (((size_t)1 << (log16_num_items * 4)) < (prepopulation_conf->num_items + 1))
        log16_num_items++;
    size_t key_position_step = prepopulation_conf->key_length / log16_num_items;
    if (key_position_step == 0)
        key_position_step = 1;
    // printf("%lu %lu %lu\n", key_position_step, log16_num_items, prepopulation_conf->key_length);
    assert(key_position_step * log16_num_items <= prepopulation_conf->key_length);

    uint64_t key_index;
    uint64_t key_index_last_report = 0;
    for (key_index = 0; key_index < prepopulation_conf->num_items; key_index++)
    {
        if ((key_index & 0xff) == 0)
        {
            uint64_t t_end = mehcached_stopwatch_now();
            if (t_end - t_last_report >= mehcached_stopwatch_1_sec)
            {
                double tput = (double)(key_index - key_index_last_report) / mehcached_stopwatch_diff_in_s(t_end, t_last_report);
                printf("prepopulation: %3.2lf%% @ %2.2lf M items/sec\n", 100. * (double)key_index / (double)prepopulation_conf->num_items, tput / 1000000.);
                fflush(stdout);
                key_index_last_report = key_index;
                t_last_report += mehcached_stopwatch_1_sec;
            }
        }

        uint64_t key_hash = mehcached_hash_key(key_index);
        uint16_t partition_id = mehcached_get_partition_id(state, key_hash);
        uint8_t owner_thread_id = server_conf->partitions[partition_id].thread_id;

        // if (owner_thread_id != thread_id)
        //     continue;
        if (rte_lcore_to_socket_id(owner_thread_id) != node_id)
            continue;

        // from netbench_client.c

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

        *(uint64_t *)value = (key_index & 0xffffffff) | ((~key_index & 0xffffffff) << 32);

#ifdef NETBENCH_SERVER_MEHCACHED
        struct mehcached_table *partition = state->partitions[partition_id];
        uint8_t alloc_id;
        if (partition_id < server_conf->num_partitions)
            alloc_id = (uint8_t)((MEHCACHED_CONCURRENT_TABLE_WRITE(server_conf, partition_id) && !MEHCACHED_CONCURRENT_ALLOC_WRITE(server_conf, partition_id)) ? thread_id : 0);
        else
            alloc_id = 0;
        uint32_t expire_time = 0;
        bool overwrite = false;
        if (mehcached_set(alloc_id, partition, key_hash, (const uint8_t *)key, key_length, (const uint8_t *)value, prepopulation_conf->value_length, expire_time, overwrite))
#endif
#ifdef NETBENCH_SERVER_MEMCACHED
        if (process_update((char *)key, key_length, (char *)value, prepopulation_conf->value_length, false, true, false))
#endif
#ifdef NETBENCH_SERVER_MASSTREE
        if (masstree_put(state->masstree, thread_id, (const char *)key, key_length, (const char *)value, prepopulation_conf->value_length))
#endif
#ifdef NETBENCH_SERVER_RAMCLOUD
        if (ramcloud_put(state->ramcloud, thread_id, (const char *)key, key_length, (const char *)value, prepopulation_conf->value_length))
#endif
        {
            ;
        }
        else
        {
            fprintf(stderr, "failed to insert key %lu on core %hhu\n", key_index, thread_id);
        }
    }
    // if (thread_id == 0)
    //     printf("\n");

    return 0;
}

static
void
mehcached_benchmark_server(const char *machine_filename, const char *server_name, int cpu_mode, int port_mode, const char *prepopulation_filename)
{
    struct mehcached_server_conf *server_conf = mehcached_get_server_conf(machine_filename, server_name);
    struct mehcached_prepopulation_conf *prepopulation_conf = mehcached_get_prepopulation_conf(prepopulation_filename, server_name);

    mehcached_stopwatch_init_start();

    printf("initializing shm\n");

    const size_t page_size = 1048576 * 2;
    const size_t num_numa_nodes = 2;
    const size_t num_pages_to_try = 16384;
    const size_t num_pages_to_reserve = 16384 - 2048;	// give 2048 pages to dpdk

    mehcached_shm_init(page_size, num_numa_nodes, num_pages_to_try, num_pages_to_reserve);

    printf("initializing DPDK\n");

    uint64_t cpu_mask = 0;
    uint8_t thread_id;
	for (thread_id = 0; thread_id < server_conf->num_threads; thread_id++)
		cpu_mask |= (uint64_t)1 << thread_id;
    char cpu_mask_str[100];
    snprintf(cpu_mask_str, sizeof(cpu_mask_str), "%lx", cpu_mask);

    char memory_str[10];
    snprintf(memory_str, sizeof(memory_str), "%zu", (num_pages_to_try - num_pages_to_reserve) * 2);   // * 2 is because the used huge page size is 2 MB

    char *rte_argv[] = {"",
        "-c", cpu_mask_str,
        "-n", "4",    // 4 for server 
        "-m", memory_str,
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
	for (port_id = 0; port_id < server_conf->num_ports; port_id++)
		port_mask |= (uint64_t)1 << port_id;
    if (!mehcached_init_network(cpu_mask, port_mask, &num_ports_max))
    {
        fprintf(stderr, "failed to initialize network\n");
        return;
    }
    assert(server_conf->num_ports <= num_ports_max);


    printf("setting MAC address\n");
    for (port_id = 0; port_id < server_conf->num_ports; port_id++)
    {
        struct ether_addr mac_addr;
        memcpy(&mac_addr, server_conf->ports[port_id].mac_addr, sizeof(struct ether_addr));
        if (rte_eth_dev_mac_addr_add(port_id, &mac_addr, 0) != 0)
        {
            fprintf(stderr, "failed to add a MAC address\n");
            return;
        }
    }


    printf("configuring mappings\n");

    uint16_t partition_id;
#ifdef USE_HOT_ITEMS
    uint8_t hot_item_id;
#endif

    for (port_id = 0; port_id < server_conf->num_ports; port_id++)
    {
        if (!mehcached_set_dst_port_mask(port_id, 0xffff))
            return;
    }

    for (partition_id = 0; partition_id < server_conf->num_partitions; partition_id++)
    {
        server_conf->partitions[partition_id].thread_id %= (uint8_t)(server_conf->num_threads >> cpu_mode);
        uint8_t thread_id = server_conf->partitions[partition_id].thread_id;
        for (port_id = 0; port_id < server_conf->num_ports; port_id++)
            if (!mehcached_set_dst_port_mapping(port_id, (uint16_t)partition_id, thread_id % (uint8_t)(server_conf->num_threads >> cpu_mode)))
                return;
    }

    for (thread_id = 0; thread_id < server_conf->num_threads; thread_id++)
    {
        for (port_id = 0; port_id < server_conf->num_ports; port_id++)
            if (!mehcached_set_dst_port_mapping(port_id, (uint16_t)(1024 + thread_id), thread_id % (uint8_t)(server_conf->num_threads >> cpu_mode)))
                return;
    }

#ifdef USE_HOT_ITEMS
    for (hot_item_id = 0; hot_item_id < server_conf->num_hot_items; hot_item_id++)
    {
        server_conf->hot_items[hot_item_id].thread_id %= (uint8_t)(server_conf->num_threads >> cpu_mode);
        uint8_t thread_id = server_conf->hot_items[hot_item_id].thread_id;
        for (port_id = 0; port_id < server_conf->num_ports; port_id++)
            if (!mehcached_set_dst_port_mapping(port_id, (uint16_t)(2048 + hot_item_id), thread_id))
                return;
    }
#endif


    printf("cleaning up pending packets\n");
    for (thread_id = 1; thread_id < server_conf->num_threads; thread_id++)
        rte_eal_launch(mehcached_benchmark_consume_packets_proc, (void *)(size_t)server_conf->num_ports, (unsigned int)thread_id);
    rte_eal_launch(mehcached_benchmark_consume_packets_proc, (void *)(size_t)server_conf->num_ports, 0);

    rte_eal_mp_wait_lcore();

    for (port_id = 0; port_id < server_conf->num_ports; port_id++)
        rte_eth_stats_reset(port_id);

#else

    uint16_t partition_id;

#endif

    printf("initializing server states\n");

    size_t mem_start = mehcached_get_memuse();

    struct server_state *states[server_conf->num_threads];
#ifdef NETBENCH_SERVER_MEHCACHED
    struct mehcached_table *partitions[server_conf->num_partitions + server_conf->num_threads];
#endif

    for (thread_id = 0; thread_id < server_conf->num_threads; thread_id++)
    {
        struct server_state *state = mehcached_shm_malloc_contiguous(sizeof(struct server_state), thread_id);
        states[thread_id] = state;
        memset(state, 0, sizeof(struct server_state));

        state->server_conf = server_conf;
        state->prepopulation_conf = prepopulation_conf;
#ifdef NETBENCH_SERVER_MEHCACHED
        state->partitions = partitions;
#endif

        state->target_request_rate = 20000000;  // 20 Mops

#ifdef MEHCACHED_USE_IB
		state->next_tx_pkt = 0;
#endif

#ifdef MEHCACHED_USE_SOFT_FDIR
        size_t mailbox_index;
        size_t ring_size = 256;
        // for (mailbox_index = 0; mailbox_index < MEHCACHED_MAX_THREADS; mailbox_index++)
        for (mailbox_index = 0; mailbox_index < MEHCACHED_MAX_NUMA_NODES; mailbox_index++)
        {
            char ring_name[64];
            snprintf(ring_name, sizeof(ring_name), "soft_fdir_mailbox_%hhu_%zu", thread_id, mailbox_index);
            // struct rte_ring *ring = rte_ring_create(ring_name, (unsigned int)ring_size, (int)rte_lcore_to_socket_id(thread_id), RING_F_SP_ENQ | RING_F_SC_DEQ);
            struct rte_ring *ring = rte_ring_create(ring_name, (unsigned int)ring_size, (int)mailbox_index, RING_F_SC_DEQ);
            if (ring == NULL)
            {
                fprintf(stderr, "failed to allocate soft_fdir mailbox %zu on thread %hhu\n", mailbox_index, thread_id);
                return;
            }
            state->soft_fdir_mailbox[mailbox_index] = ring;
        }
#endif
        state->cpu_mode = cpu_mode;
        state->port_mode = port_mode;
    }

#ifdef NETBENCH_SERVER_MEHCACHED
    for (partition_id = 0; partition_id < server_conf->num_partitions; partition_id++)
    {
        uint8_t thread_id = server_conf->partitions[partition_id].thread_id;

        //uint8_t num_allocs = (uint8_t)((MEHCACHED_CONCURRENT_TABLE_WRITE(server_conf, partition_id) && !MEHCACHED_CONCURRENT_ALLOC_WRITE(server_conf, partition_id)) ? (server_conf->num_threads >> 1) : 1);
        uint8_t num_allocs = (uint8_t)((MEHCACHED_CONCURRENT_TABLE_WRITE(server_conf, partition_id) && !MEHCACHED_CONCURRENT_ALLOC_WRITE(server_conf, partition_id)) ? server_conf->num_threads : 1);
        uint64_t num_items = server_conf->partitions[partition_id].num_items;
        uint64_t alloc_size = server_conf->partitions[partition_id].alloc_size;
        double mth_threshold = server_conf->partitions[partition_id].mth_threshold;

        // consider per-item overhead
        size_t alloc_overhead = sizeof(struct mehcached_item);
#ifdef MEHCACHED_ALLOC_DYNAMIC
        alloc_overhead += MEHCAHCED_DYNAMIC_OVERHEAD;
#endif
        alloc_size += alloc_overhead * num_items;

        if (num_allocs > 1)
        {
            alloc_size /= num_allocs;
            // we need much larger pools for concurrent_table_write && !concurrent_alloc_write because the server cannot perform in-place updates well (due to different alloc_id), which incurs less efficient use of logs
            alloc_size = alloc_size * 2;    // 100% larger size; comparable success rate for uniform requests (94.2% for CRCW vs 97.1% for EREW/CREW)
        }

        // use larger space to compensate space efficiency
        num_items = num_items * 12 / 10;
        alloc_size = alloc_size * 12 / 10;

        partitions[partition_id] = mehcached_shm_malloc_contiguous(sizeof(struct mehcached_table), thread_id);

        size_t table_numa_node = rte_lcore_to_socket_id((unsigned int)thread_id);
        size_t alloc_numa_nodes[num_allocs];
        uint8_t alloc_id;
        for (alloc_id = 0; alloc_id < num_allocs; alloc_id++)
            alloc_numa_nodes[alloc_id] = table_numa_node;

        bool concurrent_table_read = MEHCACHED_CONCURRENT_TABLE_READ(server_conf, partition_id);
        bool concurrent_table_write = MEHCACHED_CONCURRENT_TABLE_WRITE(server_conf, partition_id);
        bool concurrent_alloc_write = MEHCACHED_CONCURRENT_ALLOC_WRITE(server_conf, partition_id);
        mehcached_table_init(partitions[partition_id],
            (num_items + MEHCACHED_ITEMS_PER_BUCKET - 1) / MEHCACHED_ITEMS_PER_BUCKET,
            num_allocs,
            alloc_size,
            concurrent_table_read, concurrent_table_write, concurrent_alloc_write,
            table_numa_node, alloc_numa_nodes,
            mth_threshold);
    }
    for (thread_id = 0; thread_id < server_conf->num_threads; thread_id++)
    {
        uint16_t partition_id = (uint16_t)(server_conf->num_partitions + thread_id);

        uint8_t num_allocs = 1;
        uint64_t num_items = 256;
        uint64_t alloc_size = 2048576;

        partitions[partition_id] = mehcached_shm_malloc_contiguous(sizeof(struct mehcached_table), thread_id);

        size_t table_numa_node = rte_lcore_to_socket_id((unsigned int)thread_id);
        size_t alloc_numa_nodes[num_allocs];
        uint8_t alloc_id;
        for (alloc_id = 0; alloc_id < num_allocs; alloc_id++)
            alloc_numa_nodes[alloc_id] = table_numa_node;

        // CREW for hot items
        bool concurrent_table_read = true;
        bool concurrent_table_write = false;
        bool concurrent_alloc_write = false;
        mehcached_table_init(partitions[partition_id],
            (num_items + MEHCACHED_ITEMS_PER_BUCKET - 1) / MEHCACHED_ITEMS_PER_BUCKET,
            num_allocs,
            alloc_size,
            concurrent_table_read, concurrent_table_write, concurrent_alloc_write,
            table_numa_node, alloc_numa_nodes,
            MEHCACHED_MTH_THRESHOLD_FIFO);
    }
#endif
#ifdef NETBENCH_SERVER_MEMCACHED
    {
        uint64_t total_num_items = 0;
        uint64_t total_alloc_size = 0;
        for (partition_id = 0; partition_id < server_conf->num_partitions; partition_id++)
        {
            total_num_items += server_conf->partitions[partition_id].num_items;
            total_alloc_size += server_conf->partitions[partition_id].alloc_size;
        }

        // memcached uses quite much more space if the average item size is large
        if (total_alloc_size / total_num_items >= 64)
            total_alloc_size = total_alloc_size * 13 / 10;

        // consider per-item overhead
        total_alloc_size += 88 * total_num_items;

        uint64_t t = server_conf->num_threads;
        char t_str[64];
        sprintf(t_str, "%lu", t);
        printf("t = %lu\n", t);

        uint64_t m = total_alloc_size / 1048576;
        char m_str[64];
        sprintf(m_str, "%lu", m);
        printf("m = %lu\n", m);

        uint64_t hashpower = 0;
        while (((uint64_t)1 << hashpower) < total_num_items)
            hashpower++;
        hashpower -= 2; // each bucket has 4 slots
        hashpower += 1; // avoid full hash table (memc3 will abort)
        char hashpower_str[64];
        sprintf(hashpower_str, "hashpower=%lu", hashpower);
        printf("hashpower = %lu\n", hashpower);

        const char *argv[] = {"(internal)", "-v", "-u", "root", "-L", "-C", "-m", m_str, "-o", hashpower_str};
        int argc = sizeof(argv) / sizeof(argv[0]);
        if (init(argc, (char **)argv) != 0)
            fprintf(stderr, "failed to initialize memcached\n");
    }
#endif
#ifdef NETBENCH_SERVER_MASSTREE
    {
#ifndef NETBENCH_SERVER_MASSTREE_P
        masstree_t t = masstree_init(server_conf->num_threads, false);
#else
        masstree_t t = masstree_init(server_conf->num_threads, true);
#endif
        for (thread_id = 0; thread_id < server_conf->num_threads; thread_id++)
            states[thread_id]->masstree = t;
    }
#endif
#ifdef NETBENCH_SERVER_RAMCLOUD
    {
        ramcloud_t t = ramcloud_init(server_conf->num_threads);
        for (thread_id = 0; thread_id < server_conf->num_threads; thread_id++)
            states[thread_id]->ramcloud = t;
    }
#endif

    mehcached_stopwatch_init_end();

    printf("prepopulating servers\n");

    for (thread_id = 0; thread_id < server_conf->num_threads; thread_id++) {
		if (rte_lcore_to_socket_id(thread_id) == 0) {
			rte_eal_launch(mehcached_benchmark_prepopulate_proc, states, thread_id);
			break;
		}
	}
    rte_eal_mp_wait_lcore();
    for (thread_id = 0; thread_id < server_conf->num_threads; thread_id++) {
		if (rte_lcore_to_socket_id(thread_id) == 1) {
			rte_eal_launch(mehcached_benchmark_prepopulate_proc, states, thread_id);
			break;
		}
	}
    rte_eal_mp_wait_lcore();

    size_t mem_diff = mehcached_get_memuse() - mem_start;
    printf("memory:   %10.2lf MB\n", (double)mem_diff * 0.000001);

    printf("running servers\n");

    struct sigaction new_action;
    new_action.sa_handler = signal_handler;
    sigemptyset(&new_action.sa_mask);
    new_action.sa_flags = 0;
    sigaction(SIGINT, &new_action, NULL);
    sigaction(SIGTERM, &new_action, NULL);

    // use this for diagnosis (the actual server will not be run)
    // mehcached_diagnosis(server_conf);

    for (thread_id = 1; thread_id < server_conf->num_threads; thread_id++)
        rte_eal_launch(mehcached_benchmark_server_proc, states, (unsigned int)thread_id);
    rte_eal_launch(mehcached_benchmark_server_proc, states, 0);

    rte_eal_mp_wait_lcore();


    // mehcached_free_network(port_mask);

    printf("finished\n");
}

int
main(int argc, const char *argv[])
{
#ifndef MEHCACHED_MEASURE_LATENCY
    if (argc < 6)
    {
        printf("%s MACHINE-FILENAME SERVER-NAME CPU-MODE PORT-MODE PREPOPULATION-FILENAME [TARGET-REQUEST-RATE]\n", argv[0]);
        return EXIT_FAILURE;
    }
	if (argc < 7)
		target_request_rate_from_user = 0;
	else
		target_request_rate_from_user = (uint32_t)atoi(argv[6]);
#else
    if (argc < 7)
    {
        printf("%s MACHINE-FILENAME SERVER-NAME CPU-MODE PORT-MODE PREPOPULATION-FILENAME TARGET-REQUEST-RATE\n", argv[0]);
        return EXIT_FAILURE;
    }
    target_request_rate_from_user = (uint32_t)atoi(argv[6]);
#endif


    mehcached_benchmark_server(argv[1], argv[2], atoi(argv[3]), atoi(argv[4]), argv[5]);

    return EXIT_SUCCESS;
}

