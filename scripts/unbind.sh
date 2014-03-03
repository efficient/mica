#!/bin/bash

export RTE_SDK=`readlink -f $(dirname ${BASH_SOURCE[0]})/../../DPDK`

DEVS=`lspci | grep 82599EB | awk '{ print $1 }'`

sudo $RTE_SDK/tools/pci_unbind.py --bind=ixgbe $DEVS

