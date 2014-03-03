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

#ifndef __PERF_COUNT__
#define __PERF_COUNT__

#ifdef __cplusplus
extern "C" {
#endif

#include <stddef.h>
#include <stdint.h>

enum PERF_COUNT_TYPE 
{
	PERF_COUNT_TYPE_HW_CPU_CYCLES,
	PERF_COUNT_TYPE_HW_INSTRUCTIONS,
	PERF_COUNT_TYPE_HW_CACHE_REFERENCES,
	PERF_COUNT_TYPE_HW_CACHE_MISSES,
	PERF_COUNT_TYPE_HW_BRANCH_INSTRUCTIONS,
	PERF_COUNT_TYPE_HW_BRANCH_MISSES,
	PERF_COUNT_TYPE_HW_BUS_CYCLES,
	PERF_COUNT_TYPE_HW_CACHE_L1I_READ_ACCESS,
	PERF_COUNT_TYPE_HW_CACHE_L1I_READ_MISS,
	PERF_COUNT_TYPE_HW_CACHE_L1I_WRITE_ACCESS,
	PERF_COUNT_TYPE_HW_CACHE_L1I_WRITE_MISS,
	PERF_COUNT_TYPE_HW_CACHE_L1I_PREFETCH_ACCESS,		// not working?
	PERF_COUNT_TYPE_HW_CACHE_L1I_PREFETCH_MISS,			// not working?
	PERF_COUNT_TYPE_HW_CACHE_L1D_READ_ACCESS,
	PERF_COUNT_TYPE_HW_CACHE_L1D_READ_MISS,
	PERF_COUNT_TYPE_HW_CACHE_L1D_WRITE_ACCESS,
	PERF_COUNT_TYPE_HW_CACHE_L1D_WRITE_MISS,
	PERF_COUNT_TYPE_HW_CACHE_L1D_PREFETCH_ACCESS,		// not working?
	PERF_COUNT_TYPE_HW_CACHE_L1D_PREFETCH_MISS,			// not working?
	PERF_COUNT_TYPE_HW_CACHE_LL_READ_ACCESS,
	PERF_COUNT_TYPE_HW_CACHE_LL_READ_MISS,
	PERF_COUNT_TYPE_HW_CACHE_LL_WRITE_ACCESS,
	PERF_COUNT_TYPE_HW_CACHE_LL_WRITE_MISS,
	PERF_COUNT_TYPE_HW_CACHE_LL_PREFETCH_ACCESS,		// not working?
	PERF_COUNT_TYPE_HW_CACHE_LL_PREFETCH_MISS,			// not working?
	PERF_COUNT_TYPE_HW_CACHE_ITLB_READ_ACCESS,
	PERF_COUNT_TYPE_HW_CACHE_ITLB_READ_MISS,
	PERF_COUNT_TYPE_HW_CACHE_ITLB_WRITE_ACCESS,
	PERF_COUNT_TYPE_HW_CACHE_ITLB_WRITE_MISS,
	PERF_COUNT_TYPE_HW_CACHE_ITLB_PREFETCH_ACCESS,		// not working?
	PERF_COUNT_TYPE_HW_CACHE_ITLB_PREFETCH_MISS,		// not working?
	PERF_COUNT_TYPE_HW_CACHE_DTLB_READ_ACCESS,
	PERF_COUNT_TYPE_HW_CACHE_DTLB_READ_MISS,
	PERF_COUNT_TYPE_HW_CACHE_DTLB_WRITE_ACCESS,
	PERF_COUNT_TYPE_HW_CACHE_DTLB_WRITE_MISS,
	PERF_COUNT_TYPE_HW_CACHE_DTLB_PREFETCH_ACCESS,		// not working?
	PERF_COUNT_TYPE_HW_CACHE_DTLB_PREFETCH_MISS,		// not working?
	PERF_COUNT_TYPE_SW_CPU_CLOCK,
	PERF_COUNT_TYPE_SW_TASK_CLOCK,
	PERF_COUNT_TYPE_SW_PAGE_FAULTS,
	PERF_COUNT_TYPE_SW_CONTEXT_SWITCHES,
	PERF_COUNT_TYPE_SW_CPU_MIGRATIONS,
	PERF_COUNT_TYPE_SW_PAGE_FAULTS_MIN,
	PERF_COUNT_TYPE_SW_PAGE_FAULTS_MAJ,
	PERF_COUNT_TYPE_SW_ALIGNMENT_FAULTS,
	PERF_COUNT_TYPE_SW_EMULATION_FAULTS,

	PERF_COUNT_TYPE_MAX,

	PERF_COUNT_TYPE_INVALID = -1,
};

typedef void *perf_count_t;

// system_wide would require CAP_SYS_ADMIN
perf_count_t perf_count_init(const enum PERF_COUNT_TYPE *perf_count_types, size_t num_events, int system_wide);
void perf_count_free(perf_count_t perf_count);

void perf_count_start(perf_count_t perf_count);
void perf_count_stop(perf_count_t perf_count);
void perf_count_reset(perf_count_t perf_count);

uint64_t perf_count_get_by_index(perf_count_t perf_count, size_t index);
uint64_t perf_count_get_by_type(perf_count_t perf_count, enum PERF_COUNT_TYPE type);

const char *perf_count_name_by_type(enum PERF_COUNT_TYPE type);
enum PERF_COUNT_TYPE perf_count_type_by_name(const char *name);

#ifdef __cplusplus
}
#endif

#endif
