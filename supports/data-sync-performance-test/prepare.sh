#!/usr/bin/env bash

. ./config

case=$1
# delete historical data
echo "delete data in remote cluster."
ssh ${remote_namenode} "hdfs dfs -rm -r /${case}; hdfs dfs -mkdir /${case}"

drop_cache="sync;echo 3 > /proc/sys/vm/drop_caches"
# drop cache for all cluster hosts
echo "drop cache for ${hosts}."
for host in ${hosts}; do
  ssh $host "${drop_cache}"
done