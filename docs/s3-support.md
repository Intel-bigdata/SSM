# SSM S3 Support

SSM has already supported AWS S3 in `Copy2S3Action` action. Now we can set a remote S3 path as a destination in 'copy2s3' action, e.g.,

```
copy2s3 -file {hdfs_src} -dest {s3_dest}
```

Note that `{s3_dest}` should start with `s3a` rather than `s3`. You can find more information about `s3a` in [1].

## Copy file from HDFS to S3 with SSM

### Add S3 configuration in SSM

SSM has already solved the dependency of S3. We only need to add some configuration in `{SSM_HOME}/conf/smart-site.xml`:

```xml
<property>
        <name>fs.s3a.access.key</name>
        <value>{s3.key}</value>
</property>
<property>
        <name>fs.s3a.secret.key</name>
        <value>{s3.secret}</value>
</property>
```

where `{s3.key}` and `{s3.secret}` are the key and secret of your S3 account.

### Copy to S3 with action

We can use the following action to copy file from HDFS to AWS S3:

```shell
copy2s3 -file {src} -dest {dest}
```

Note that `{src}` and `{dest}` should be full path, e.g., `hdfs://test/1.txt` and `s3a://test/1.txt`.

Here is an example in SSM WebUI command:

```shell
copy2s3 -file /test/copytest -dest s3a://{test_dir}/copytest
```

### Copy to S3 with rule (Experimental)

```shell
file: path matches "/{hdfs_dir}/*" | copy2s3 -dest {s3_dir}
```

where `{hdfs_dir}` and `{s3_dir}` are the HDFS source direstory and S3 targe direstory.

## Enable S3 support in HDFS（Optional）

This is an optional step. We highly recommand enable this feature on HDFS for test and trouble shooting. The hadoop version we use is 2.7.

### Solve HDFS S3 dependency

First, we need to copy some jar packages about `aws` and `jackson` from `${HADOOP_HOME}/share/hadoop/tools/lib}` to `${HADOOP_HOME}/share/hadoop/tools/common/lib`. The dependencies we need are listed below:

```
aws-java-sdk-1.7.4.jar
hadoop-aws-2.7.4.jar
jackson-annotations-2.2.3.jar
jackson-core-2.2.3.jar
jackson-core-asl-1.9.13.jar
jackson-databind-2.2.3.jar
jackson-jaxrs-1.9.13.jar
jackson-mapper-asl-1.9.13.jar
jackson-xc-1.9.13.jar
```

The versions of the jar packages are related to Hadoop version. We can find these jar dependencies in `${HADOOP_HOME}/share/hadoop/tools/lib}` by using commands like this:

```shell
ll | grep jackson
ll | grep aws
```

### Add AWS S3 configuration in HDFS

Then, we need to add some configurations in `${HADOOP_HOME}/etc/hadoop/core-site.xml`:

```xml
<property>
        <name>fs.s3a.access.key</name>
        <value>{s3.key}</value>
</property>
<property>
        <name>fs.s3a.secret.key</name>
        <value>{s3.secret}</value>
</property>
```

### Use S3 File System in HDFS command

We can use the command `hdfs dfs -ls s3a://{test_dir}/` to list the file in remote AWS S3 cluster. Then, you can see output like this:

```shell
$ hdfs dfs -ls s3a://{test_dir}/
Found 2 items
-rw-rw-rw-   1       2048 2017-11-19 23:47 s3a://{test_dir}/1.txt
-rw-rw-rw-   1          1 2017-11-21 18:58 s3a://{test_dir}/1511319531258
```

## References

1. [S3 Support in Apache Hadoop](https://wiki.apache.org/hadoop/AmazonS3)
2. [Hadoop-AWS module: Integration with Amazon Web Services](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html)
3. [Using the S3A FileSystem Client](https://hortonworks.github.io/hdp-aws/s3-s3aclient/index.html)