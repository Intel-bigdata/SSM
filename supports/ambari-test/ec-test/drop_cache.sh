#!/usr/bin/env bash

. ./config

# drop cache for hosts specified in config.
drop_cache="sync;echo 3 > /proc/sys/vm/drop_caches"
echo "drop cache for ${HOSTS}."
for host in ${HOSTS}; do
  ssh $host "${drop_cache}"
done
