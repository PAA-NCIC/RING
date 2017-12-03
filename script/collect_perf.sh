#!/bin/bash

rm -rf perf
mkdir perf
cd perf

FROM="/home/mengke/test/test_rdma/rdma/build/src/*.prof"

mkdir blade12
TO="./blade12"
scp mengke@blade12:$FROM $TO

mkdir blade13
TO="./blade13"
scp mengke@blade13:$FROM $TO

mkdir blade14
TO="./blade14"
scp mengke@blade14:$FROM $TO

mkdir blade15
TO="./blade15"
scp mengke@blade15:$FROM $TO

mkdir blade16
TO="./blade16"
scp mengke@blade16:$FROM $TO

mkdir blade17
TO="./blade17"
scp mengke@blade17:$FROM $TO

mkdir blade18
TO="./blade18"
scp mengke@blade18:$FROM $TO

mkdir blade19
TO="./blade19"
scp mengke@blade19:$FROM $TO

mkdir blade20
TO="./blade20"
scp mengke@blade20:$FROM $TO
