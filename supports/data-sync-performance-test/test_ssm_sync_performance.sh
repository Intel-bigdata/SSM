#!/usr/bin/env bash
# avoid blocking REST API
unset http_proxy
# for python use
export PYTHONPATH=../integration-test:$PYTHONPATH

. config
echo "------------------ Your configuration ------------------"
echo "SSM home is ${SMART_HOME}."
echo "PAT home is ${PAT_HOME}."
echo "Test cases are ${cases}."
echo "Dest cluster is ${DEST_CLUSTER}."
echo "--------------------------------------------------------"

bin=$(dirname "${BASH_SOURCE-$0}")
bin=$(cd "${bin}">/dev/null; pwd)

# make ssm log empty before test
echo "" > ${SMART_HOME}/logs/smartserver.log

for case in ${cases}; do
    for i in {1..5}; do
        sh prepare.sh ${case}
        echo "==================== test case: $case, test round: $i ============================"
        pushd ${PAT_HOME}/PAT-collecting-data
        echo "export PYTHONPATH=${bin}/../integration-test:${PYTHONPATH}; python ${bin}/run_ssm_sync.py ${case} ${i} ${DEST_CLUSTER}" > cmd.sh
        ./pat run "$case-$i"
        cp ${SMART_HOME}/logs/smartserver.log ./results/${case}-${i}.log
        popd
    done
done