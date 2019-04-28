#!/usr/bin/env bash



. ./config



# mkdir and ec
for size in "${!CASES[@]}"; do

    num=${CASES[$size]}

    dir="${size}_${num}"

    # delete historical data

    echo "delete data in dest dir."

    # unset ec policy

    ssh ${DEST_NODE} "hdfs dfs -rm -r /dest/${dir}; hdfs dfs -mkdir /dest/${dir}"

    ssh ${DEST_NODE} "hdfs ec -setPolicy -path /dest/${dir}"   

done

