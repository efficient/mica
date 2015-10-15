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

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include "util.h"

struct trace_replay_state
{
	uint64_t n;			// number of items (input)

	size_t cdf_n;
	double *cdf;
	uint32_t *item_sizes;

	uint32_t avg_item_size;

	size_t index_size;
	uint32_t *index;

	// unsigned short rand_state[3];		// prng state
	uint64_t rand_state;

	bool copy;
};

static
void
mehcached_trace_init(struct trace_replay_state *state, const char *path, size_t index_size, uint64_t n, uint64_t rand_seed)
{
	assert(n > 0);
	assert(rand_seed < (1UL << 48));
	memset(state, 0, sizeof(struct trace_replay_state));

	// load CDF and item sizes
	FILE *fp = fopen(path, "r");
	size_t i;
	double cdf;
	uint32_t item_size;
	while (true) {
		int c = fscanf(fp, "%lf %u\n", &cdf, &item_size);
		if (c != 2)
			break;
		i++;
	}
	state->cdf_n = i;
	state->cdf = (double *)malloc(sizeof(double) * (state->cdf_n + 1));
	state->item_sizes = (uint32_t *)malloc(sizeof(uint32_t) * state->cdf_n);
	i = 0;
	fseek(fp, 0, SEEK_SET);
	while (true) {
		int c = fscanf(fp, "%lf %u\n", &cdf, &item_size);
		if (c != 2)
			break;
		state->cdf[i - 1] = 1. - cdf;
		state->item_sizes[i] = item_size;
		i++;
	}
	state->cdf[i] = 1.;	// sentinel
	fclose(fp);

	// calculate the average item size
	double avg_item_size = 0.;
	for (i = 0; i < state->cdf_n; i++)
		avg_item_size += (state->cdf[i + 1] - state->cdf[i]) * (double)state->item_sizes[i];
	state->avg_item_size = (uint32_t)(avg_item_size + 0.5);

	// build an index
	state->index_size = index_size;
	state->index = (uint32_t *)malloc(sizeof(uint32_t) * (state->index_size + 1));
	size_t cdf_i = 0;
	for (i = 0; i < state->index_size; i++)
	{
		double v = (double)i / (double)state->index_size;
		while (cdf_i + 1 < state->cdf_n && state->cdf[i + 1] < v)
			cdf_i++;
		state->index[i] = (uint32_t)cdf_i;
	}
	state->index[i] = (uint32_t)(state->cdf_n - 1);	// sentinel

	assert(n >= state->cdf_n);
	state->n = n;

	// state->rand_state[0] = (unsigned short)(rand_seed >> 0);
	// state->rand_state[1] = (unsigned short)(rand_seed >> 16);
	// state->rand_state[2] = (unsigned short)(rand_seed >> 32);
	state->rand_state = rand_seed;

	state->copy = false;

	printf("mehcached_trace_init: state size: %zu bytes (CDF, item size) + %zu bytes (index)\n", sizeof(double) * (state->cdf_n + 1) + sizeof(uint32_t) * state->cdf_n, sizeof(uint32_t) * (state->index_size + 1));
}

static
void
mehcached_trace_init_copy(struct trace_replay_state *state, const struct trace_replay_state *src_state, uint64_t rand_seed)
{
	assert(rand_seed < (1UL << 48));
	memcpy(state, src_state, sizeof(struct trace_replay_state));
	// state->rand_state[0] = (unsigned short)(rand_seed >> 0);
	// state->rand_state[1] = (unsigned short)(rand_seed >> 16);
	// state->rand_state[2] = (unsigned short)(rand_seed >> 32);
	state->rand_state = rand_seed;
	state->copy = true;
}

static
void
mehcached_trace_free(struct trace_replay_state *state)
{
	if (!state->copy)
	{
		free(state->cdf);
		free(state->item_sizes);
		free(state->index);
	}
}

static
void
mehcached_trace_change_n(struct trace_replay_state *state, uint64_t n)
{
	assert(n >= cdf_n);
	state->n = n;
}

static
uint64_t
mehcached_trace_next(struct trace_replay_state *state, uint32_t *item_size)
{
	// double u = erand48(state->rand_state);
	double u = mehcached_rand_d(&state->rand_state);

	size_t cdf_i = state->index[(size_t)((double)state->index_size * u)];
	
	while (cdf_i + 1 < state->cdf_n && state->cdf[cdf_i + 1] < u)
		cdf_i++;
	
	uint64_t p_start_quantized = (uint64_t)((double)state->n * state->cdf[cdf_i]);
	uint64_t p_end_quantized = (uint64_t)((double)state->n * state->cdf[cdf_i + 1]);
	assert(p_start_quantized < p_end_quantized);

	uint64_t key = p_start_quantized + (uint64_t)((double)(p_end_quantized - p_start_quantized) * (u - state->cdf[cdf_i]));
	if (item_size != NULL)
		*item_size = state->item_sizes[cdf_i];

	return key;
}

static
void
mehcached_test_trace(const char *path, size_t index_size)
{
	const uint64_t n = 1000000UL;
	uint64_t i;

	struct trace_replay_state state;
	mehcached_trace_init(&state, path, index_size, n, 0);

	uint64_t num_key0 = 0;
	const uint64_t num_samples = 10000000UL;

	for (i = 0; i < num_samples; i++)
		if (mehcached_trace_next(&state, NULL) == 0)
			num_key0++;

	printf("index_size = %zu: %.10lf", index_size, (double)num_key0 / (double)num_samples);
	printf("\n");
}
