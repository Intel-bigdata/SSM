#! /bin/bash

HADOOP_HOME=/home/sorttest/hadoop
WORKSPACE=/home/ssm

cd $HADOOP_HOME
./sbin/stop-all.sh
clean-input-up
hadoop namenode -format
./sbin/start-all.sh

echo "Wait 10s for Hadoop to start..."
sleep 10s
cd $WORKSPACE
echo "Write files..."
hdfs dfs -mkdir /testArchive
hdfs dfs -mkdir /testCache
hdfs dfs -mkdir /testAllSsd

hdfs storagepolicies -setStoragePolicy -path /testArchive -policy ALL_SSD
hdfs storagepolicies -setStoragePolicy -path /testCache -policy COLD
hdfs storagepolicies -setStoragePolicy -path /testAllSsd -policy COLD

hdfs dfs -put data_1GB /testArchive/testArchiveFile
hdfs dfs -put data_1GB /testCache/testCacheFile
hdfs dfs -put data_1GB /testAllSsd/testAllSsdFile

hdfs dfs -mkdir /testFileAccessCount
hdfs dfs -put smalldata /testFileAccessCount/file1
hdfs dfs -put smalldata /testFileAccessCount/file2
hdfs dfs -put smalldata /testFileAccessCount/file3
hdfs dfs -put smalldata /testFileAccessCount/file4
hdfs dfs -put smalldata /testFileAccessCount/file5

# echo "Drop cache..."
# drop-cache
