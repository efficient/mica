#!/bin/bash

export RTE_SDK=`readlink -f $(dirname ${BASH_SOURCE[0]})/../../DPDK`

DEVS=`lspci | grep '82599\|X520' | awk '{ print $1 }'`

#sudo $RTE_SDK/tools/pci_unbind.py --bind=ixgbe $DEVS	# 1.5
#sudo $RTE_SDK/tools/igb_uio_bind.py --bind=ixgbe $DEVS	# 1.6
sudo $RTE_SDK/tools/dpdk_nic_bind.py --bind=ixgbe $DEVS	# 1.8
