#!/bin/bash
#./run2.sh top_ten_MapReduce_b.py /data/ethereum/contracts /data/ethereum/transactions /users/twm31/out
./scripts/run2-local.sh ./scripts/top_ten_MapReduce_b.py /data/ethereum/contracts /data/ethereum/transactions ./output/part-b-out.txt
#python top_ten_MapReduce_b.py -r hadoop hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/contracts hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/transactions > part_b_out.txt 
