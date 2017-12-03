#!/bin/bash
PWD="/home/mengke/test/test_rdma/rdma"
bash $PWD/script/bcast_.sh $2
mpiexec -n $1 -f $PWD/script/host $PWD/build/application/$2
