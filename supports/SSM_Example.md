# SSM Demo Cases

## Preparations for Demo

Create two kinds of test files with different sizes.

```
fallocate -l 1G data_1GB
fallocate -l 6K smalldata
```
Clear HDFS metadata.

```
rm -rf /tmp/hadoop-root/dfs/name
#mkdir -p /tmp/hadoop-root/dfs/name
rm -rf /tmp/hadoop-root/dfs/data
#mkdir -p /tmp/hadoop-root/dfs/data
rm -rf /tmp/hadoop-root/dfs/data1
#mkdir -p /tmp/hadoop-root/dfs/data1
rm -rf /tmp/hadoop-root/dfs/data2
#mkdir -p /tmp/hadoop-root/dfs/data2
rm -rf /tmp/hadoop-root/dfs/data3
#mkdir -p /tmp/hadoop-root/dfs/data3
rm -rf /tmp/hadoop-root/dfs/data4
#mkdir -p /tmp/hadoop-root/dfs/data4
```

Prepare files on HDFS.

```bash
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
```


## Rules
Add rules with GUI. Then, trigger these rules with actions.

**Rule1:** 

Watch `/testCache/`, cache files if conditions are satisfied.

```
file : path matches "/testCache/*" and accessCount(40s) > 1 | cache 
```

**Tigger Example 1**

```
read -file /testCache/testCacheFile
```

**Rule2:** 

Watch `/testAllSsd/`, move files to all ssd if conditions are satisified.

```
file : path matches "/testAllSsd/*" and accessCount(40s) > 1 | allssd 

```

**Tigger Example 2**

```
read -file /testAllSsd/testAllSsdFile
```

**Rule3:** 

Watch `/testArchive/`, archive files if conditions are satisifyied.

```
file : path matches "/testArchive/*" and age > 10min | archive 
```

**Tigger Example 3**

```
-file /testArchive/testArchiveFile
```

## Actions
Use Actions to read/write files, and generate access events. Then, you can use if previous added rules are triggered.

**Basic Actions and usage:**

- hello: Print message. `hello -print_message {message}` 
- read: Read file once. `read -file {filePath}`
- write: Create a new file with given size. `write -file {filePath} -length {size}` 
- checkstorage: Check file Storage statuses. `checkstorage -file {filePath}`
- cache: Cache file. `cache -file {filePath}`
- archive: Move file to cold storage. `archive -file {filePath}`
- allssd: Move file to all SSD storage. `allssd -file {filePath}`
- onessd: Move file to one SSD storage. `onessd -file {filePath}`

