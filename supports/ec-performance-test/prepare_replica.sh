#!/usr/bin/env bash

. ./config

# delete historical data and mkdir
for size in "${!CASES[@]}"; do
    num=${CASES[$size]}
    dir="${size}_${num}"
    ssh ${SRC_NODE} "hdfs dfs -rm -r ${DEST_DIR_REPLICA}/${dir}; hdfs dfs -mkdir ${DEST_DIR_REPLICA}/${dir}"
done

