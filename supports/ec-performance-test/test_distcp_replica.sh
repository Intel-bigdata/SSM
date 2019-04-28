#!/usr/bin/env bash



echo "Get configuration from config."

. config



echo "------------------ Your configuration ------------------"

echo "PAT home is ${PAT_HOME}."

echo "Test case:"

for size in ${!CASES[@]}; do

  echo ${size} ${CASES[$size]}

done

echo "--------------------------------------------------------"



bin=$(dirname "${BASH_SOURCE-$0}")

bin=$(cd "${bin}">/dev/null; pwd)

log="${bin}/distcp.log"

# remove historical data in log file

printf "" > ${log}


 # 3 replica
    for size in "${!CASES[@]}"; do

        case=${size}_${CASES[$size]}

        printf "Test case ${case} with ${MAPPER_NUM} mappers:\n replica\n" >> ${log}

        for i in {1..1}; do

            echo "==================== test case: $case, mapper num: ${MAPPER_NUM}, test round: $i ============================"

            sh drop_cache.sh

            sh prepare_replica.sh

            cd ${PAT_HOME}/PAT-collecting-data

            echo "hadoop distcp -skipcrccheck -m ${MAPPER_NUM} ${DEST_CLUSTER}/dest/${case}/* ${SRC_CLUSTER}/replica/${case}/"

            echo "start_time=\`date +%s\`;\

            hadoop distcp -skipcrccheck -m ${MAPPER_NUM} ${DEST_CLUSTER}/dest/${case}/* ${SRC_CLUSTER}/replica/${case}/ > results/$case_${MAPPER_NUM}_$i.log 2>&1;\

            end_time=\`date +%s\`;\

            printf \"\$((end_time-start_time))s \" >> ${log}" > cmd.sh

            ./pat run "${case}_"replica"_${MAPPER_NUM}_${i}"

            cd ${bin}

        done

        printf "\nTest case ${case} with $m mapper is finished!\n" >> ${log}

    done

