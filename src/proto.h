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
#include "table.h"

#include <rte_ether.h>
#include <rte_ip.h>
#include <rte_udp.h>

// override opaque to perform client-side throttling
#define MEHCACHED_ENABLE_THROTTLING

// override expire time to measure end-to-end latency
// use this only for full-RX latency measurement using core 0; this may lower throughput due to more I/O and processing on core0 and less responsive throttling
//#define MEHCACHED_MEASURE_LATENCY

// (ETHER_MAX_LEN - ETHER_CRC_LEN - sizeof(struct mehcached_batch_packet)) / (sizeof(struct mehcached_request) + 8 + 8)
#define MEHCACHED_MAX_BATCH_SIZE (36)

// use software flow director (slower); this does not disable hardware flow director on the server, but the client will send packets to all cores regardless of the concurrency mode
//#define MEHCACHED_USE_SOFT_FDIR

// collect per-partition load
#define MEHCACHED_COLLECT_PER_PARTITION_LOAD

struct mehcached_batch_packet
{
	// 0
	uint8_t header[sizeof(struct ether_hdr) + sizeof(struct ipv4_hdr) + sizeof(struct udp_hdr)];
	// 42
	uint8_t num_requests;
	uint8_t reserved0;
	// 44
	uint32_t opaque;
	// 48
    uint8_t data[0];
    // batch
};
