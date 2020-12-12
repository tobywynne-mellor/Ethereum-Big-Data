#!/bin/bash

echo 'rm -r '$4
rm -r $4 

echo 'python '$1' -r hadoop hdfs://andromeda.eecs.qmul.ac.uk'$2' hdfs://andromeda.eecs.qmul.ac.uk'$3' > '$4''
python $1 -r hadoop hdfs://andromeda.eecs.qmul.ac.uk$2 hdfs://andromeda.eecs.qmul.ac.uk$3 > $4
