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
#include <rte_cycles.h>

extern uint64_t mehcached_stopwatch_1_sec;		// this should be used whenever possible for better accuracy than below
extern uint64_t mehcached_stopwatch_1_msec;
extern uint64_t mehcached_stopwatch_1_usec;

void
mehcached_stopwatch_init_start();

void
mehcached_stopwatch_init_end();

static
uint64_t
mehcached_stopwatch_now()
{
	return rte_rdtsc();
}

static
uint64_t
mehcached_stopwatch_diff_in_us(uint64_t new_t, uint64_t old_t)
{
	return (new_t - old_t) * 1000000UL / mehcached_stopwatch_1_sec;
}

static
double
mehcached_stopwatch_diff_in_s(uint64_t new_t, uint64_t old_t)
{
	return (double)mehcached_stopwatch_diff_in_us(new_t, old_t) * 0.000001;
}