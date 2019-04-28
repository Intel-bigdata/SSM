#!/usr/bin/env bash



. ./config



# mkdir and ec
for size in "${!CASES[@]}"; do

    num=${CASES[$size]}

    dir="${size}_${num}"

    # delete historical data

    echo "delete data in dest dir."

    ssh ${SRC_NODE} "hdfs ec -setPolicy -path /${dir}"

    ssh ${SRC_NODE} "hdfs dfs -rm -r /replica/${dir}; hdfs dfs -mkdir /replica/${dir}"

done

