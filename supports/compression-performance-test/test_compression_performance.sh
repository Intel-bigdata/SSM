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
echo "HiBench home is ${HiBench_HOME}"
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


# Test DFSIO and Terasort continuously for 3 rounds
for size in "${!CASES[@]}"; do    
    case="${size}_${CASES[$size]}"

    # Read TestDFSIO
        action="readDFSIO"
	echo "Test case ${case}($action):" >> ${log}
        echo "==================== test case: $action ============================"
	sh drop_cache.sh
        # make ssm log empty before test
        printf "" > ${SMART_HOME}/logs/smartserver.log
        cd ${PAT_HOME}/PAT-collecting-data
        # hdfs user will execute the cmd, you can change it to the one in your test env. with execution permission
        echo "su hdfs -c \"hadoop jar ${HADOOP_HOME}/hadoop-mapreduce/hadoop-mapreduce-client-jobclient-*-tests.jar \
        TestDFSIO -Dtest.build.data=/\"${size}_${CASES[$size]}\" \
        -read -nrFiles ${CASES[$size]} -size ${size} -resFile /home/hdfs/dfsio_compress_test.log\"" > cmd.sh
         chmod 755 cmd.sh
        ./pat run "${case}_${action}"
        cd ${bin}
	# Read Compress TestDFSIO
        action="compressDFSIO"
	echo "Test case ${case}($action):" >> ${log}
        echo "==================== test case: $action ============================"
	sh drop_cache.sh
        # make ssm log empty before test
        printf "" > ${SMART_HOME}/logs/smartserver.log
        cd ${PAT_HOME}/PAT-collecting-data
        echo "export PYTHONPATH=${bin}/../integration-test:${PYTHONPATH};\
         python ${bin}/run_ssm_compress.py ${size} ${CASES[$size]} ${log} ${action};\
        su hdfs -c \"hadoop jar ${HADOOP_HOME}/hadoop-mapreduce/hadoop-mapreduce-client-jobclient-*-tests.jar \
        TestDFSIO -Dtest.build.data=/\"${size}_${CASES[$size]}\" \
        -read -nrFiles ${CASES[$size]} -size ${size} -resFile /home/hdfs/dfsio_mover_test.log\"" > cmd.sh
         chmod 755 cmd.sh
        ./pat run "${case}_${action}"
        cd ${bin}
        # Terasort
	action="Terasort"
	echo "Test case ($action):" >> ${log}
        echo "==================== test case: $action ============================"
        sh drop_cache.sh
        cd ${PAT_HOME}/PAT-collecting-data
        echo "\chmod 777 -R ${HiBench_HOME};\
                su hdfs -c \"sh ${HiBench_HOME}/bin/workloads/micro/terasort/hadoop/run.sh\"" > cmd.sh
        ./pat run "${action}"
        cd ${bin}
         # Compress Terasort
	action="compressTerasort"
	echo "Test case ($action):" >> ${log}
        echo "==================== test case: $action ============================"
        sh drop_cache.sh
        cd ${PAT_HOME}/PAT-collecting-data
        echo "export PYTHONPATH=${bin}/../integration-test:${PYTHONPATH};\
         python ${bin}/run_ssm_compress.py ${size} ${CASES[$size]} ${log} ${action};\
         su hdfs -c \"sh ${HiBench_HOME}/bin/workloads/micro/terasort/hadoop/run.sh\"" > cmd.sh
        ./pat run "${action}"
        cd ${bin}
    done
    printf "\nTest case ${case} is finished!\n" >> ${log}
done

