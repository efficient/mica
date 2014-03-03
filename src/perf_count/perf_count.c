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

#include "perf_count.h"

#ifdef __cplusplus
extern "C" {
#endif

#ifdef __linux__
#include <linux/perf_event.h>
#include <sys/types.h>
#include <sys/syscall.h>
#endif

#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

struct perf_count_ctx
{
	size_t num_groups;
	size_t num_events;
	enum PERF_COUNT_TYPE *types;
#ifdef __linux__
	struct perf_event_attr *events;
#endif
	int *fds;
	uint64_t *counters;
};

static const char *perf_count_name[] = 
{
	"CPUCycles",
	"Instructions",
	"CacheReferences",
	"CacheMisses",
	"BranchInstructions",
	"BranchMisses",
	"BUSCycles",
	"L1IReadAccess",
	"L1IReadMiss",
	"L1IWriteAccess",
	"L1IWriteMiss",
	"L1IPrefetchAccess",
	"L1IPrefetchMiss",
	"L1DReadAccess",
	"L1DReadMiss",
	"L1DWriteAccess",
	"L1DWriteMiss",
	"L1DPrefetchAccess",
	"L1DPrefetchMiss",
	"LLReadAccess",
	"LLReadMiss",
	"LLWriteAccess",
	"LLWriteMiss",
	"LLPrefetchAccess",
	"LLPrefetchMiss",
	"ITLBReadAccess",
	"ITLBReadMiss",
	"ITLBWriteAccess",
	"ITLBWriteMiss",
	"ITLBPrefetchAccess",
	"ITLBPrefetchMiss",
	"DTLBReadAccess",
	"DTLBReadMiss",
	"DTLBWriteAccess",
	"DTLBWriteMiss",
	"DTLBPrefetchAccess",
	"DTLBPrefetchMiss",
	"CPUClock",
	"TaskClock",
	"PageFaults",
	"ContextSwitches",
	"CPUMigrations",
	"PageFaultsMinor",
	"PageFaultsMajor",
	"AlignmentFaults",
	"EmulationFaults",
};

#ifdef __linux__
static const struct perf_event_attr perf_count_mapping[] =
{
	{ .type = PERF_TYPE_HARDWARE, .config = PERF_COUNT_HW_CPU_CYCLES },
	{ .type = PERF_TYPE_HARDWARE, .config = PERF_COUNT_HW_INSTRUCTIONS },
	{ .type = PERF_TYPE_HARDWARE, .config = PERF_COUNT_HW_CACHE_REFERENCES },
	{ .type = PERF_TYPE_HARDWARE, .config = PERF_COUNT_HW_CACHE_MISSES },
	{ .type = PERF_TYPE_HARDWARE, .config = PERF_COUNT_HW_BRANCH_INSTRUCTIONS },
	{ .type = PERF_TYPE_HARDWARE, .config = PERF_COUNT_HW_BRANCH_MISSES },
	{ .type = PERF_TYPE_HARDWARE, .config = PERF_COUNT_HW_BUS_CYCLES },
	{ .type = PERF_TYPE_HW_CACHE, .config = PERF_COUNT_HW_CACHE_L1I  | (PERF_COUNT_HW_CACHE_OP_READ << 8)     | (PERF_COUNT_HW_CACHE_RESULT_ACCESS << 16) },
	{ .type = PERF_TYPE_HW_CACHE, .config = PERF_COUNT_HW_CACHE_L1I  | (PERF_COUNT_HW_CACHE_OP_READ << 8)     | (PERF_COUNT_HW_CACHE_RESULT_MISS << 16) },
	{ .type = PERF_TYPE_HW_CACHE, .config = PERF_COUNT_HW_CACHE_L1I  | (PERF_COUNT_HW_CACHE_OP_WRITE << 8)    | (PERF_COUNT_HW_CACHE_RESULT_ACCESS << 16) },
	{ .type = PERF_TYPE_HW_CACHE, .config = PERF_COUNT_HW_CACHE_L1I  | (PERF_COUNT_HW_CACHE_OP_WRITE << 8)    | (PERF_COUNT_HW_CACHE_RESULT_MISS << 16) },
	{ .type = PERF_TYPE_HW_CACHE, .config = PERF_COUNT_HW_CACHE_L1I  | (PERF_COUNT_HW_CACHE_OP_PREFETCH << 8) | (PERF_COUNT_HW_CACHE_RESULT_ACCESS << 16) },
	{ .type = PERF_TYPE_HW_CACHE, .config = PERF_COUNT_HW_CACHE_L1I  | (PERF_COUNT_HW_CACHE_OP_PREFETCH << 8) | (PERF_COUNT_HW_CACHE_RESULT_MISS << 16) },
	{ .type = PERF_TYPE_HW_CACHE, .config = PERF_COUNT_HW_CACHE_L1D  | (PERF_COUNT_HW_CACHE_OP_READ << 8)     | (PERF_COUNT_HW_CACHE_RESULT_ACCESS << 16) },
	{ .type = PERF_TYPE_HW_CACHE, .config = PERF_COUNT_HW_CACHE_L1D  | (PERF_COUNT_HW_CACHE_OP_READ << 8)     | (PERF_COUNT_HW_CACHE_RESULT_MISS << 16) },
	{ .type = PERF_TYPE_HW_CACHE, .config = PERF_COUNT_HW_CACHE_L1D  | (PERF_COUNT_HW_CACHE_OP_WRITE << 8)    | (PERF_COUNT_HW_CACHE_RESULT_ACCESS << 16) },
	{ .type = PERF_TYPE_HW_CACHE, .config = PERF_COUNT_HW_CACHE_L1D  | (PERF_COUNT_HW_CACHE_OP_WRITE << 8)    | (PERF_COUNT_HW_CACHE_RESULT_MISS << 16) },
	{ .type = PERF_TYPE_HW_CACHE, .config = PERF_COUNT_HW_CACHE_L1D  | (PERF_COUNT_HW_CACHE_OP_PREFETCH << 8) | (PERF_COUNT_HW_CACHE_RESULT_ACCESS << 16) },
	{ .type = PERF_TYPE_HW_CACHE, .config = PERF_COUNT_HW_CACHE_L1D  | (PERF_COUNT_HW_CACHE_OP_PREFETCH << 8) | (PERF_COUNT_HW_CACHE_RESULT_MISS << 16) },
	{ .type = PERF_TYPE_HW_CACHE, .config = PERF_COUNT_HW_CACHE_LL   | (PERF_COUNT_HW_CACHE_OP_READ << 8)     | (PERF_COUNT_HW_CACHE_RESULT_ACCESS << 16) },
	{ .type = PERF_TYPE_HW_CACHE, .config = PERF_COUNT_HW_CACHE_LL   | (PERF_COUNT_HW_CACHE_OP_READ << 8)     | (PERF_COUNT_HW_CACHE_RESULT_MISS << 16) },
	{ .type = PERF_TYPE_HW_CACHE, .config = PERF_COUNT_HW_CACHE_LL   | (PERF_COUNT_HW_CACHE_OP_WRITE << 8)    | (PERF_COUNT_HW_CACHE_RESULT_ACCESS << 16) },
	{ .type = PERF_TYPE_HW_CACHE, .config = PERF_COUNT_HW_CACHE_LL   | (PERF_COUNT_HW_CACHE_OP_WRITE << 8)    | (PERF_COUNT_HW_CACHE_RESULT_MISS << 16) },
	{ .type = PERF_TYPE_HW_CACHE, .config = PERF_COUNT_HW_CACHE_LL   | (PERF_COUNT_HW_CACHE_OP_PREFETCH << 8) | (PERF_COUNT_HW_CACHE_RESULT_ACCESS << 16) },
	{ .type = PERF_TYPE_HW_CACHE, .config = PERF_COUNT_HW_CACHE_LL   | (PERF_COUNT_HW_CACHE_OP_PREFETCH << 8) | (PERF_COUNT_HW_CACHE_RESULT_MISS << 16) },
	{ .type = PERF_TYPE_HW_CACHE, .config = PERF_COUNT_HW_CACHE_ITLB | (PERF_COUNT_HW_CACHE_OP_READ << 8)     | (PERF_COUNT_HW_CACHE_RESULT_ACCESS << 16) },
	{ .type = PERF_TYPE_HW_CACHE, .config = PERF_COUNT_HW_CACHE_ITLB | (PERF_COUNT_HW_CACHE_OP_READ << 8)     | (PERF_COUNT_HW_CACHE_RESULT_MISS << 16) },
	{ .type = PERF_TYPE_HW_CACHE, .config = PERF_COUNT_HW_CACHE_ITLB | (PERF_COUNT_HW_CACHE_OP_WRITE << 8)    | (PERF_COUNT_HW_CACHE_RESULT_ACCESS << 16) },
	{ .type = PERF_TYPE_HW_CACHE, .config = PERF_COUNT_HW_CACHE_ITLB | (PERF_COUNT_HW_CACHE_OP_WRITE << 8)    | (PERF_COUNT_HW_CACHE_RESULT_MISS << 16) },
	{ .type = PERF_TYPE_HW_CACHE, .config = PERF_COUNT_HW_CACHE_ITLB | (PERF_COUNT_HW_CACHE_OP_PREFETCH << 8) | (PERF_COUNT_HW_CACHE_RESULT_ACCESS << 16) },
	{ .type = PERF_TYPE_HW_CACHE, .config = PERF_COUNT_HW_CACHE_ITLB | (PERF_COUNT_HW_CACHE_OP_PREFETCH << 8) | (PERF_COUNT_HW_CACHE_RESULT_MISS << 16) },
	{ .type = PERF_TYPE_HW_CACHE, .config = PERF_COUNT_HW_CACHE_DTLB | (PERF_COUNT_HW_CACHE_OP_READ << 8)     | (PERF_COUNT_HW_CACHE_RESULT_ACCESS << 16) },
	{ .type = PERF_TYPE_HW_CACHE, .config = PERF_COUNT_HW_CACHE_DTLB | (PERF_COUNT_HW_CACHE_OP_READ << 8)     | (PERF_COUNT_HW_CACHE_RESULT_MISS << 16) },
	{ .type = PERF_TYPE_HW_CACHE, .config = PERF_COUNT_HW_CACHE_DTLB | (PERF_COUNT_HW_CACHE_OP_WRITE << 8)    | (PERF_COUNT_HW_CACHE_RESULT_ACCESS << 16) },
	{ .type = PERF_TYPE_HW_CACHE, .config = PERF_COUNT_HW_CACHE_DTLB | (PERF_COUNT_HW_CACHE_OP_WRITE << 8)    | (PERF_COUNT_HW_CACHE_RESULT_MISS << 16) },
	{ .type = PERF_TYPE_HW_CACHE, .config = PERF_COUNT_HW_CACHE_DTLB | (PERF_COUNT_HW_CACHE_OP_PREFETCH << 8) | (PERF_COUNT_HW_CACHE_RESULT_ACCESS << 16) },
	{ .type = PERF_TYPE_HW_CACHE, .config = PERF_COUNT_HW_CACHE_DTLB | (PERF_COUNT_HW_CACHE_OP_PREFETCH << 8) | (PERF_COUNT_HW_CACHE_RESULT_MISS << 16) },
	{ .type = PERF_TYPE_SOFTWARE, .config = PERF_COUNT_SW_CPU_CLOCK },
	{ .type = PERF_TYPE_SOFTWARE, .config = PERF_COUNT_SW_TASK_CLOCK },
	{ .type = PERF_TYPE_SOFTWARE, .config = PERF_COUNT_SW_PAGE_FAULTS },
	{ .type = PERF_TYPE_SOFTWARE, .config = PERF_COUNT_SW_CONTEXT_SWITCHES },
	{ .type = PERF_TYPE_SOFTWARE, .config = PERF_COUNT_SW_CPU_MIGRATIONS },
	{ .type = PERF_TYPE_SOFTWARE, .config = PERF_COUNT_SW_PAGE_FAULTS_MIN },
	{ .type = PERF_TYPE_SOFTWARE, .config = PERF_COUNT_SW_PAGE_FAULTS_MAJ },
	{ .type = PERF_TYPE_SOFTWARE, .config = PERF_COUNT_SW_ALIGNMENT_FAULTS },
	{ .type = PERF_TYPE_SOFTWARE, .config = PERF_COUNT_SW_EMULATION_FAULTS },
};
#endif

#ifdef __linux__
static int
sys_perf_event_open(struct perf_event_attr *attr, pid_t pid, int cpu, int group_fd, unsigned long flags)
{
	attr->size = sizeof(*attr);
	return (int)syscall(__NR_perf_event_open, attr, pid, cpu, group_fd, flags);
}
#endif

perf_count_t
perf_count_init(const enum PERF_COUNT_TYPE *perf_count_types, size_t num_events, int system_wide)
{
#ifdef __linux__
	if (perf_count_types == NULL)
		return NULL;

	for (size_t event = 0; event < num_events; event++)
		if (perf_count_types[event] < 0 || perf_count_types[event] >= PERF_COUNT_TYPE_MAX)
			return NULL;

	struct perf_count_ctx *ctx = (struct perf_count_ctx *)malloc(sizeof(struct perf_count_ctx));
	if (!ctx)
		return NULL;

	if (system_wide)
		ctx->num_groups = (size_t)sysconf(_SC_NPROCESSORS_ONLN);
	else
		ctx->num_groups = 1;
	ctx->num_events = num_events;

	ctx->types = (enum PERF_COUNT_TYPE *)calloc(sizeof(enum PERF_COUNT_TYPE), (size_t)ctx->num_events);
	if (!ctx->types)
	{
		free(ctx);
		return NULL;
	}
	ctx->events = (struct perf_event_attr *)calloc(sizeof(struct perf_event_attr), (size_t)ctx->num_events);
	if (!ctx->events)
	{
		free(ctx->types);
		free(ctx);
		return NULL;
	}
	ctx->fds = (int *)calloc(sizeof(int), (size_t)ctx->num_groups * (size_t)ctx->num_events);
	if (!ctx->fds)
	{
		free(ctx->events);
		free(ctx->types);
		free(ctx);
		return NULL;
	}
	ctx->counters = (uint64_t *)calloc(sizeof(uint64_t), (size_t)ctx->num_events);
	if (!ctx->counters)
	{
		free(ctx->fds);
		free(ctx->events);
		free(ctx->types);
		free(ctx);
		return NULL;
	}

	for (size_t event = 0; event < ctx->num_events; event++)
	{
		ctx->types[event] = perf_count_types[event];
		ctx->events[event] = perf_count_mapping[perf_count_types[event]];
		ctx->events[event].read_format = PERF_FORMAT_TOTAL_TIME_ENABLED | PERF_FORMAT_TOTAL_TIME_RUNNING;
		if (!system_wide)
			ctx->events[event].inherit = 1;
	}

	for (size_t group = 0; group < ctx->num_groups; group++)
		for (size_t event = 0; event < ctx->num_events; event++)
		{
			pid_t pid;
			int cpu;

			if (system_wide)
			{
				pid = -1;
				// XXX: assuming the IDs of online cpus range from 0 to (num_cpus - 1)
				cpu = (int)group;
			}
			else
			{
				// this process
				pid = 0;
				cpu = -1;
			}

			ctx->fds[group * ctx->num_events + event] = sys_perf_event_open(&ctx->events[event], pid, cpu, -1, 0);
			if (ctx->fds[group * ctx->num_events + event] < 0)
			{
				perror("perf_count: error while sys_perf_event_open()");
				break;
			}
		}

	return ctx;
#else
	return NULL;
#endif
}

void
perf_count_free(perf_count_t perf_count)
{
#ifdef __linux__
	struct perf_count_ctx *ctx = (struct perf_count_ctx *)perf_count;

	for (size_t group = 0; group < ctx->num_groups; group++)
		for (size_t event = 0; event < ctx->num_events; event++)
		{
			if (ctx->fds[group * ctx->num_events + event] >= 0)
				close(ctx->fds[group * ctx->num_events + event]);
		}

	free(ctx->counters);
	free(ctx->fds);
	free(ctx->events);
	free(ctx->types);
	free(ctx);
#endif
}

static void
perf_count_accumulate(perf_count_t perf_count, int additive)
{
#ifdef __linux__
	struct perf_count_ctx *ctx = (struct perf_count_ctx *)perf_count;

	for (size_t event = 0; event < ctx->num_events; event++)
	{
		uint64_t count[3];
		uint64_t accum_count[3] = {0, 0, 0};

		for (size_t group = 0; group < ctx->num_groups; group++)
		{
			if (ctx->fds[group * ctx->num_events + event] < 0)
				continue;

			count[0] = count[1] = count[2] = 0;
			ssize_t len = read(ctx->fds[group * ctx->num_events + event], count, sizeof(count));
			//printf("%d %ld %ld %ld\n", len, count[0], count[1], count[2]);
			if (len < 0)
			{
				perror("perf_count: error while reading stats");
				break;
			}
			else if ((size_t)len != sizeof(count))
			{
				fprintf(stderr, "perf_count: invalid stats reading; did you really use -std=gnu99 when compiling?\n");
				break;
			}

			accum_count[0] += count[0];
			accum_count[1] += count[1];
			accum_count[2] += count[2];
		}

		if (accum_count[2] == 0)
		{
			// no event occurred at all
		}
		else
		{
			if (accum_count[2] < accum_count[1])
			{
				// need to scale
				accum_count[0] = (uint64_t)((double)accum_count[0] * (double)accum_count[1] / (double)accum_count[2] + 0.5);
			}
		}

		if (additive)
		{
			ctx->counters[event] += accum_count[0];
			// due to the scaling, we may observe a negative increment
			if ((int64_t)ctx->counters[event] < 0)
				ctx->counters[event] = 0;
		}
		else
			ctx->counters[event] -= accum_count[0];
	}
#endif
}

void
perf_count_start(perf_count_t perf_count)
{
	perf_count_accumulate(perf_count, 0);
}

void
perf_count_stop(perf_count_t perf_count)
{
	perf_count_accumulate(perf_count, 1);
}

void
perf_count_reset(perf_count_t perf_count)
{
#ifdef __linux__
	struct perf_count_ctx *ctx = (struct perf_count_ctx *)perf_count;

	for (size_t event = 0; event < ctx->num_events; event++)
		ctx->counters[event] = 0;
#endif
}

uint64_t
perf_count_get_by_index(perf_count_t perf_count, size_t index)
{
#ifdef __linux__
	struct perf_count_ctx *ctx = (struct perf_count_ctx *)perf_count;

	if (index >= ctx->num_events)
		return (uint64_t)-1;

	return ctx->counters[index];
#else
	return (uint64_t)-1;
#endif
}

uint64_t
perf_count_get_by_type(perf_count_t perf_count, enum PERF_COUNT_TYPE type)
{
#ifdef __linux__
	if (type < 0 || type >= PERF_COUNT_TYPE_MAX)
		return (uint64_t)-1;

	struct perf_count_ctx *ctx = (struct perf_count_ctx *)perf_count;

	for (size_t event = 0; event < ctx->num_events; event++)
		if (ctx->types[event] == type)
			return ctx->counters[event];
#endif

	return (uint64_t)-1;
}

const char *
perf_count_name_by_type(enum PERF_COUNT_TYPE type)
{
#ifdef __linux__
	if (type < 0 || type >= PERF_COUNT_TYPE_MAX)
		return NULL;

	return perf_count_name[type];
#else
	return NULL;
#endif
}

enum PERF_COUNT_TYPE
perf_count_type_by_name(const char *name)
{
#ifdef __linux__
	if (!name)
		return PERF_COUNT_TYPE_INVALID;

	for (size_t type = 0; type < PERF_COUNT_TYPE_MAX; type++)
		if (strcmp(perf_count_name[type], name) == 0)
			return type;
#endif

	return PERF_COUNT_TYPE_INVALID;
}

#ifdef __cplusplus
}
#endif

