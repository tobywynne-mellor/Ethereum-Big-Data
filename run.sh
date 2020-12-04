#!/bin/bash

echo 'hadoop fs -rm -r '$3
hadoop fs -rm -r $3 

echo 'python '$1' -r hadoop --output-dir '$3' --no-cat-output hdfs://andromeda.eecs.qmul.ac.uk'$2
python $1 -r hadoop --output-dir $3 --no-cat-output hdfs://andromeda.eecs.qmul.ac.uk$2
