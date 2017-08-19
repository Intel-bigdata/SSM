#! /bin/bash

echo "Reset Integration Test Env..."
echo "Remove local files..."
rm data_10MB
rm data_64MB
rm data_10GB
rm data_2GB
rm data_10GB
echo "Generate local files..."
fallocate -l 10MB data_10MB
fallocate -l 64MB data_64MB
fallocate -l 1G data_1GB
fallocate -l 2G data_2GB
fallocate -l 10G data_10GB

echo "Remove Test files on HDFS..."
# Remove test files on HDFS
hdfs dfs -rmr /test

echo "Upload Test files to HDFS..."
hdfs dfs -put data_10MB /test/data_10MB
hdfs dfs -put data_64MB /test/data_64MB
hdfs dfs -put data_1GB /test/data_1GB
hdfs dfs -put data_2GB /test/data_2GB
hdfs dfs -put data_10GB /test/data_10GB

# hdfs storagepolicies -setStoragePolicy -path /testArchive -policy ALL_SSD
# hdfs storagepolicies -setStoragePolicy -path /testCache -policy COLD
# hdfs storagepolicies -setStoragePolicy -path /testAllSsd -policy COLD

# hdfs dfs -put data_1GB /testArchive/testArchiveFile
# hdfs dfs -put data_1GB /testCache/testCacheFile
# hdfs dfs -put data_1GB /testAllSsd/testAllSsdFile

# hdfs dfs -mkdir /testFileAccessCount
# hdfs dfs -put smalldata /testFileAccessCount/file1
# hdfs dfs -put smalldata /testFileAccessCount/file2
# hdfs dfs -put smalldata /testFileAccessCount/file3
# hdfs dfs -put smalldata /testFileAccessCount/file4
# hdfs dfs -put smalldata /testFileAccessCount/file5

# echo "Drop cache..."
# drop-cache
