#!/usr/bin/env bash



. ./config


drop_cache="sync;echo 3 > /proc/sys/vm/drop_caches"

# drop cache for all cluster hosts

echo "drop cache for ${HOSTS}."

for host in ${HOSTS}; do

  ssh $host "${drop_cache}"

done
