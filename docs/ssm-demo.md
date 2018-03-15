# SSM demo

### Requirements

1. Configure the storage type such as SSD, DISK and ARCHIVE in hdfs.xml under HDFS conf directory.
   This is an example for your reference.

```xml
<property>
    <name>dfs.datanode.data.dir</name>
    <value>[SSD]/mnt/disk_a/hadoop,[DISK]/mnt/disk_b/hadoop,[ARCHIVE]/mnt/disk_c/hadoop</value>
</property>
```

2. Configure the credential of s3 for supporting copy2s3 action. Please refer to s3-support.md.



### Rule for cache

Files: /demoCacheFile/f1, /demoCacheFile/f2, /demoCacheFile/f3

If the file under /demoCacheFile is accessed at least once during the last 1min, it will be moved to HDFS cache.
```shell
file : accessCount(1min) > 0 and path matches "/demoCacheFile/*" | cache
```

Submit a read action to trigger the condition in the above rule.
```shell
read -file /demoCacheFile/f1
```

Verify the cached file is shown on SSM's web UI by selecting Data Temperature/Files in Cache on Cluster tab.


### Rule for moving data from cold storage to hot storage

Files: /demoCold2Hot/f1, /demoCold2Hot/f2, /demoCold2Hot/f3

If the file under /demoCold2Hot is accessed at least once, it will be moved to SSD.
```shell
file : accessCount(1min) > 0 and path matches "/demoCold2Hot/*" | allssd
```

Submit a read action to trigger the condition in the above rule.
```shell
read -file /demoCold2Hot/f1
```

Submit the following action to check the shift of storage type
```shell
checkstorage -file /demoCold2Hot/f1
```

### Rule for moving data from hot storage to cold storage

Files: /demoHot2Cold/f1, /demoHot2Cold/f2, /demoHot2Cold/f3

If the age of file under /demoHot2Cold exceeds 3min, archive the file.
```shell
file : age > 3min and path matches "/demoHot2Cold/*" | archive
```

Submit the following action to check the shift of storage type
```shell
checkstorage -file /demoHot2Cold/f1
```

### Rule for sync data

Files: /demoSyncSrc/f1, /demoSyncSrc/f2, /demoSyncSrc/f3

Sync the files under /demoSyncSrc/ to another HDFS cluster.
```shell
file : path matches "/demoSyncSrc/*" | sync -dest hdfs://namenode:9000/demoSyncDest
```

Run the following hadoop command to check whether the file is synchronized.
```shell
hadoop fs -checksum /demoSyncSrc/f1
hadoop fs -checksum /demoSyncDest/f1
```

### Copy file by copy2s3 action

File: /test.txt

Copy file /test.txt to s3.
```shell
copy2s3 -file /test.txt -dest s3a://xxxctest/test.txt
```


Run the following HDFS command to check whether the file is copied to s3.
```shell
hdfs dfs -cat s3a://xxxctest/test.txt
```

