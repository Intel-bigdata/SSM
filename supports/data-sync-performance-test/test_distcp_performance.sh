#!/usr/bin/env bash

echo "Get configuration from config."
. config

echo "------------------ Your configuration ------------------"
echo "PAT home is ${PAT_HOME}."
echo "Test case:"
for size in ${!CASES[@]}; do
  echo ${size} ${CASES[$size]}
done
echo "Source cluster is ${SRC_CLUSTER}."
echo "Destination cluster is ${DEST_CLUSTER}."
echo "--------------------------------------------------------"

bin=$(dirname "${BASH_SOURCE-$0}")
bin=$(cd "${bin}">/dev/null; pwd)

for m in ${MAPPER_NUM}; do
    for size in "${!CASES[@]}"; do
        case=${size}_${CASES[$size]}
        for i in {1..5}; do
            echo "==================== test case: $case, mapper num: ${m}, test round: $i ============================"
            sh prepare.sh ${case}
            cd ${PAT_HOME}/PAT-collecting-data
            echo "hadoop distcp -m $m ${SRC_CLUSTER}/${case} ${DEST_CLUSTER}/${case} > results/$case-$m-$i.log 2>&1" > cmd.sh
            ./pat run "$case-$m-$i"
            cd ${bin}
        done
    done
done
