#!/bin/bash
for i in `seq 12 20`; do
  ssh mengke@blade$i mkdir -p $1
done

