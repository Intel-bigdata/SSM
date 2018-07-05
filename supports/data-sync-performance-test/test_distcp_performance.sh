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
log="${bin}/distcp.log"
# remove historical data in log file
printf "" > ${log}

for m in ${MAPPER_NUM}; do
    for size in "${!CASES[@]}"; do
        case=${size}_${CASES[$size]}
        printf "Test case ${case} with $m mappers:\n" > ${log}
        for i in {1..5}; do
            echo "==================== test case: $case, mapper num: ${m}, test round: $i ============================"
            sh prepare.sh ${case}
            cd ${PAT_HOME}/PAT-collecting-data
            echo "start_time=\`date +%s\`;\
            hadoop distcp -m $m ${SRC_CLUSTER}/${case} ${DEST_CLUSTER}/${case} > results/$case-${m}m-$i.log 2>&1;\
            end_time=\`date +%s\`;\
            printf \"\$((end_time-start_time))s \" >> ${log}" > cmd.sh
            ./pat run "$case-$m-$i"
            cd ${bin}
        done
        printf "\nTest case ${case} with $m mapper is finished!\n" >> ${log}
    done
done
