#!/bin/bash

echo $1
echo $2

hadoop jar bd_finalproject_2.11-1.0p.jar bigdata.Task2 -Dmapreduce.job.reduces=1 $21.out $22.out

