#!/usr/bin/env bash

. ./config

# generate test data using distcp
for size in "${!CASES[@]}"; do
    num=${CASES[$size]}
    dir="${size}_${num}"
    ssh ${REMOTE_NAMENODE} "hdfs dfs -mkdir /${dir}"    
    hadoop jar /root/rui/hadoop-3.2.0-SNAPSHOT/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-3.2.0-SNAPSHOT-tests.jar TestDFSIO -write -nrFiles $(($num)) -size ${size}
    ssh ${REMOTE_NAMENODE} "hdfs dfs -mv /benchmarks/TestDFSIO/io_data/* /"${size}_$num""
    ssh ${REMOTE_NAMENODE} "hdfs dfs -rm -r /benchmarks"
done

