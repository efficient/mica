MICA
====

A fast in-memory key-value store.


Hardware Requirements
---------------------

 * Dual CPU system
 * Intel 10 GbE NICs
 * Note: The current codebase has several assumptions on the hardware configuration of the server and clients.
         It runs ideally on a dual 12-core server with 4 quad-port 10 GbE NICs, and clients with 4 quad-port 10 GbE NICs.


Software Requrements
--------------------

 * linux x86_64 >= 3.2.0
 * gcc >= 4.6.0
 * Python >= 2.6.0
 * Intel DPDK = 1.8.0-rc1
 * bash >= 4.0.0
 * cmake >= 2.6.0
 * Hugepage (2 GiB) support


Executables
-----------

 * build/netbench_server: MICA server in cache mode (use with netbench_client)
 * build/netbench_server_store: MICA server in store mode (use with netbench_client)
 * build/netbench_server_latency: MICA server in cache mode modified for end-to-end latency measurement (use with netbench_client_latency)
 * build/netbench_server_soft_fdir: MICA server in cache mode using software-based request direction (use with netbench_client_soft_fdir)
 * build/netbench_client*: MICA clients
 * build/netbench_analysis: workload analyzer (used for generating preset configurations)
 * build/microbench: a local microbenchmark for MICA in cache mode
 * build/microbench_store: a local microbenchmark for MICA in store mode
 * build/test: a simple feature test program
 * build/load: a load factor experiment


Compiling Executables
---------------------

	# unpack DPDK as "DPDK" to the directory containing mica
	$ cd mica/build
	$ ../scripts/setup_dpdk_env.sh unified	# this uses sudo;
	$ ../configure_all.sh
	$ make


Generating Configuration Files
------------------------------

	# conf_* files determine how MICA uses system resources. build/gen_confs.py generates a preset of configuration files for a 16-core server and 12-core clients
	# in mica
	$ ./run_analysis_for_conf.py	# this uses sudo
	$ ./gen_confs.py 


Running a Server
----------------

	# in mica/build
	$ sudo ./netbench_server conf_machines_DATASET_CMODE_0.5 server 0 0 conf_prepopulation_WarmupDataSet 
	# DATASET=0,1,2 (used to determine how much memory to allocate); CMODE=EREW,CREW,CRCWS (specifies the data access mode); WarmupDataSet = 0, 1, 2, empty (use different dataset to pre-warmup memory content, empty means no warmup and key-values are inserted with empty table on server) 


Running a Client (e.g., client0)
--------------------------------

	# in mica/build
	$ sudo ./netbench_client conf_machines_DATASET_CMODE_0.5 client0 0 0 conf_workload_DATASET_SKEW_GET_PUT_0.00_1
	# DATASET=0,1,2 (specifies the dataset to use); SKEW=uniform,skewed,single (specifies the workload skew); GET/PUT=0.00,0.50,0.95,1.00 (specifies the read/write ratio)


Running a Local Microbenchmark
------------------------------

	# in mica/build
	$ sudo ./microbench CMODE SKEWNESS 0.5
	# CMODE=EREW,CREW,CRCWS (specifies the data acces mode); SKEWNESS=0(uniform),0.99(skewed),99(single) (specifies the workload skew)


Contributors
------------

 * Hyeontaek Lim (CMU)
 * Sheng Li (Intel Labs)


License
-------

	Copyright 2014, 2015 Carnegie Mellon University

	Licensed under the Apache License, Version 2.0 (the "License");
	you may not use this file except in compliance with the License.
	You may obtain a copy of the License at

	    http://www.apache.org/licenses/LICENSE-2.0

	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.

