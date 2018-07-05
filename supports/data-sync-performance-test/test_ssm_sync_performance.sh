#!/usr/bin/env bash
# avoid blocking REST API
unset http_proxy
# for python use
export PYTHONPATH=../integration-test:$PYTHONPATH

echo "Get configuration from config."
. config
echo "------------------ Your configuration ------------------"
echo "SSM home is ${SMART_HOME}."
echo "PAT home is ${PAT_HOME}."
echo "Test case:"
for size in ${!CASES[@]}; do
  echo ${size} ${CASES[$size]}
done
echo "Destination cluster is ${DEST_CLUSTER}."
echo "--------------------------------------------------------"

bin=$(dirname "${BASH_SOURCE-$0}")
bin=$(cd "${bin}">/dev/null; pwd)
log="${bin}/ssm.log"
# remove historical data in log file
printf "" > ${log}

for size in "${!CASES[@]}"; do
    case=${size}_${CASES[$size]}
    echo "Test case ${case}:" >> ${log}
    for i in {1..5}; do
        echo "==================== test case: $case, test round: $i ============================"
        # make ssm log empty before test
        printf "" > ${SMART_HOME}/logs/smartserver.log
        sh prepare.sh ${case}
        cd ${PAT_HOME}/PAT-collecting-data
        echo "export PYTHONPATH=${bin}/../integration-test:${PYTHONPATH};\
         python ${bin}/run_ssm_sync.py ${size} ${CASES[$size]} ${log} ${DEST_CLUSTER}" > cmd.sh
        ./pat run "${case}-$i"
        cp ${SMART_HOME}/logs/smartserver.log ./results/${case}-${i}.log
        cd ${bin}
    done
    printf "\nTest case ${case} is finished!\n" >> ${log}
done