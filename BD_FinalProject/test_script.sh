#!/bin/bash
echo $1

spark-submit --num-executors 10 --class bigdata.Task1 --master yarn \
bd_finalproject_2.11-0.1.jar  $1 /task1.out

spark-submit --num-executors 10 --class bigdata.Task2 --master yarn \
bd_finalproject_2.11-0.1.jar  /task1.out /task2.out

spark-submit --num-executors 10 --class bigdata.Task3 --master yarn \
bd_finalproject_2.11-0.1.jar  /task1.out /task2.out /task3.out
