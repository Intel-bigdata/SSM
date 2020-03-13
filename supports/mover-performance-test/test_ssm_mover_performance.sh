#!/usr/bin/env bash
# avoid blocking REST API
unset http_proxy
# for python use
export PYTHONPATH=../integration-test:$PYTHONPATH

echo "Get configuration from config."
source config
echo "------------------ Your configuration ------------------"
echo "SSM home is ${SMART_HOME}."
echo "PAT home is ${PAT_HOME}."
echo "Hadoop home is ${HADOOP_HOME}"
echo "Test case:"
for size in ${!CASES[@]}; do
  echo ${size} ${CASES[$size]}
done
echo "--------------------------------------------------------"

bin=$(dirname "${BASH_SOURCE-$0}")
bin=$(cd "${bin}">/dev/null; pwd)
log="${bin}/ssm.log"
# remove historical data in log file
printf "" > ${log}

echo "------------------Generate test data--------------------"
su hdfs -c "sh ${bin}/prepare.sh"
echo "--------------------------------------------------------"


# Test allssd and alldisk continuously for 3 rounds
for size in "${!CASES[@]}"; do    
    case="${size}_${CASES[$size]}"
    for i in {1..2}; do
	# allssd
        action="allssd"
	echo "Test case ${case}($action):" >> ${log}
        echo "==================== test case: $case, test round: $i ============================"
	sh drop_cache.sh
        # make ssm log empty before test
        printf "" > ${SMART_HOME}/logs/smartserver.log
        cd ${PAT_HOME}/PAT-collecting-data
        # hdfs user will execute the cmd, you can change it to the one in your test env. with execution permission
        echo "export PYTHONPATH=${bin}/../integration-test:${PYTHONPATH};\
         python ${bin}/run_ssm_mover.py ${size} ${CASES[$size]} ${log} ${action};\
        su hdfs -c \"hadoop jar ${HADOOP_HOME}/hadoop-mapreduce/hadoop-mapreduce-client-jobclient-*-tests.jar \
        TestDFSIO -Dtest.build.data=/\"${size}_${CASES[$size]}\" \
        -read -nrFiles ${CASES[$size]} -size ${size} -resFile /home/hdfs/dfsio_mover_test.log\"" > cmd.sh
         chmod 755 cmd.sh
        ./pat run "${case}_${i}_${action}"
        cd ${bin}
        # alldisk
	action="alldisk"
	echo "Test case ${case}($action):" >> ${log}
        echo "==================== test case: $case, test round: $i ============================"
        sh drop_cache.sh
        cd ${PAT_HOME}/PAT-collecting-data
        echo "export PYTHONPATH=${bin}/../integration-test:${PYTHONPATH};\
         python ${bin}/run_ssm_mover.py ${size} ${CASES[$size]} ${log} ${action};\
         su hdfs -c \"hadoop jar ${HADOOP_HOME}/hadoop-mapreduce/hadoop-mapreduce-client-jobclient-*-tests.jar \
         TestDFSIO -Dtest.build.data=/\"${size}_${CASES[$size]}\" \
         -read -nrFiles ${CASES[$size]} -size ${size} -resFile /home/hdfs/dfsio_mover_test.log\"" > cmd.sh
        ./pat run "${case}_${i}_${action}"
        cd ${bin}
    done
    printf "\nTest case ${case} is finished!\n" >> ${log}
done

