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

#include "stopwatch.h"
#include <stdio.h>
#include <sys/time.h>

static struct timeval mehcached_stopwatch_init_tv;
static uint64_t mehcached_stopwatch_init_s;

uint64_t mehcached_stopwatch_1_sec = 0UL;
uint64_t mehcached_stopwatch_1_msec = 0UL;
uint64_t mehcached_stopwatch_1_usec = 0UL;

void
mehcached_stopwatch_init_start()
{
	mehcached_stopwatch_init_s = mehcached_stopwatch_now();
	gettimeofday(&mehcached_stopwatch_init_tv, NULL);
}

void
mehcached_stopwatch_init_end()
{
    struct timeval tv_now;

	const uint64_t s_1_sec = 1000000UL;

	while (true)
	{
		gettimeofday(&tv_now, NULL);

        uint64_t diff = (uint64_t)(tv_now.tv_sec - mehcached_stopwatch_init_tv.tv_sec) * 1000000UL + (uint64_t)(tv_now.tv_usec - mehcached_stopwatch_init_tv.tv_usec);
        if (diff >= s_1_sec)
        {
			uint64_t s = mehcached_stopwatch_now();
			mehcached_stopwatch_1_sec = (s - mehcached_stopwatch_init_s) * s_1_sec / diff;
			mehcached_stopwatch_1_msec = mehcached_stopwatch_1_sec / 1000UL;
			mehcached_stopwatch_1_usec = mehcached_stopwatch_1_msec / 1000UL;
        	break;
        }
	}
}
