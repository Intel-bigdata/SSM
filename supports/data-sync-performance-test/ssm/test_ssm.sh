#!/usr/bin/env bash

for i in {1..5}
do
for name in 1MB_10000 100MB_1000
do
    sh /root/polaris/prepare.sh
    echo "====================file:$name  time:$i============================"
    echo "python /root/polaris/IT/ssm.py $name $i" > cmd.sh
    ./pat run "$name-$i"
    cp /root/smart-data-1.3.2/logs/smartserver.log "results/$name-$i.log"
done
done