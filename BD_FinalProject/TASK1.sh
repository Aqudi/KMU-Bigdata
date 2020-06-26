#!/bin/bash

echo $1
echo $2

hadoop jar bd_finalproject_2.11-1.0p.jar bigdata.Task1 -Dmapreduce.job.reduces=1 $1 $21.out

