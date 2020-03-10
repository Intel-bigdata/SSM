#!/usr/bin/env bash

. ./config

# delete historical data and set ec policy
for size in "${!CASES[@]}"; do
    num=${CASES[$size]}
    dir="${size}_${num}"
    # delete historical data
    ssh ${SRC_NODE} "hdfs dfs -rm -r ${DEST_DIR_EC}/${dir}; hdfs dfs -mkdir ${DEST_DIR_EC}/${dir}"
    # set ec policy
    ssh ${SRC_NODE} "hdfs ec -setPolicy -path ${DEST_DIR_EC}/${dir}"
done

