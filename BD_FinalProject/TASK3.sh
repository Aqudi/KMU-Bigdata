#!/bin/bash

echo $1
echo $2

spark-submit --num-executors 10 --class bigdata.Task3 --master yarn \
bd_finalproject_2.11-1.0p.jar  $21.out $22.out $23.out
