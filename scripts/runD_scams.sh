#!/bin/bash
hadoop fs -rm -r out

python D_popular_scams.py -r hadoop --output-dir out --no-cat-output hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/transactions --file ../data/scams.json

hadoop fs -getmerge out ../output/D_scams_result.csv
