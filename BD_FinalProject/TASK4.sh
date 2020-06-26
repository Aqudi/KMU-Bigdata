#!/bin/bash

echo $1
echo $2

spark-submit --num-executors 10 --class bigdata.Task4 --master yarn \
bd_finalproject_2.11-1.0p.jar  $22.out $23.out $24.out

