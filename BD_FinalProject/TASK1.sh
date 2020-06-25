#!/bin/bash
echo $1
echo $2


spark-submit --num-executors 10 --class bigdata.Task1 --master yarn \
bd_finalproject_2.11-1.0p.jar  $1 $21.out
