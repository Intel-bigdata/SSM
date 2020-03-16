#!/usr/bin/env bash

. ./config

# generate test data using DFSIO
for size in "${!CASES[@]}"; do
    num=${CASES[$size]}
    dir="${size}_${num}"
    ssh ${SRC_NODE} "hdfs dfs -mkdir -p /${dir}/io_data"
    hadoop jar $HADOOP_HOME/hadoop-mapreduce/hadoop-mapreduce-client-jobclient-*-tests.jar TestDFSIO -write -nrFiles $(($num)) -size ${size}  -resFile ${pwd}/dfsio_mover_test.log
    ssh ${SRC_NODE} "hdfs dfs -mv /benchmarks/TestDFSIO/io_data/* /"${size}_$num"/io_data"
    ssh ${SRC_NODE} "hdfs dfs -rm -r /benchmarks"
done

