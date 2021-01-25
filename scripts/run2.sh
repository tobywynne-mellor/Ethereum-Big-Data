#!/bin/bash

echo 'hadoop fs -rm -r '$4
hadoop fs -rm -r $4 

echo 'python '$1' -r hadoop --output-dir '$4' --no-cat-output hdfs://andromeda.eecs.qmul.ac.uk'$2' hdfs://andromeda.eecs.qmul.ac.uk'$3''
python $1 -r hadoop --output-dir $4 --no-cat-output hdfs://andromeda.eecs.qmul.ac.uk$2 hdfs://andromeda.eecs.qmul.ac.uk$3
#python $1 -r hadoop --output-dir hdfs://andromeda.eecs.qmul.ac.uk$4 --no-cat-output hdfs://andromeda.eecs.qmul.ac.uk$2 hdfs://andromeda.eecs.qmul.ac.uk$3
