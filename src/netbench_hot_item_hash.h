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

struct mehcached_hot_item_hash
{
};

static
void
mehcached_calc_hot_item_hash(struct mehcached_server_conf *server_conf, struct mehcached_hot_item_hash *hot_item_hash)
{
	(void)server_conf;
	(void)hot_item_hash;
}

static
uint8_t
mehcached_get_hot_item_id(struct mehcached_server_conf *server_conf, struct mehcached_hot_item_hash *hot_item_hash, uint64_t key_hash)
{
	(void)server_conf;
	(void)hot_item_hash;
	(void)key_hash;
	return (uint8_t)-1;
}
