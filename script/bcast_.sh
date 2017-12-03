#!/bin/bash
PWD="/home/mengke/test/test_rdma/rdma/build/application"

FROM="${PWD}/$1"
TO="${PWD}/$1"

for i in `seq 12 20`; do
  scp $FROM  mengke@blade$i:$TO
done

