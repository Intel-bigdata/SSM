# SSM S3 Support

SSM has already supported AWS S3 in `Copy2S3Action` action. Now we can set a remote S3 path as a destination in 'copy2s3' action, e.g.,

```
copy2s3 -file {hdfs_src} -dest {s3_dest}
```

Note that `{s3_dest}` should start with `s3a` rather than `s3`. You can find more information about `s3a` in [1].

## Copy file from HDFS to S3 with SSM

### Add S3 configuration in SSM

SSM has already solved the dependency of S3. We only need to add the following configurations in `{SSM_HOME}/conf/smart-site.xml`:

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

In above, `{s3.key}` and `{s3.secret}` are the key and secret of your S3 account.

### Add S3 Endpoint (Optional)

The default `endpoint`/`region` of aws S3 may be not fast enough for your business, or you may have a self-built object storage with S3 interface. In these cases, you need to configuration `endpoint` in `{SSM_HOME}/conf/smart-site.xml`. Here is an example,

```xml
<property>
    <name>fs.s3a.endpoint</name>
    <value>{s3.endpoint}</value>
    <description>AWS S3 endpoint to connect to. An up-to-date list is
    provided in the AWS Documentation: regions and endpoints. Without this
    property, the standard region (s3.amazonaws.com) is assumed.
    </description>
</property>
```

where {s3.endpoint} is domain or IP of your S3 endpoint, e.g., `s3.ap-southeast-1.amazonaws.com` (an region of aws S3), or `{IP}:{port}` (your self-built S3 service). Note that `{port}` is not essential if your S3 service is using standard http or https port.

### Add S3 Proxy (Optional)

Sometimes, you may need a proxy to visit S3 service. In that case, you need to add the following configuration to `{SSM_HOME}/conf/smart-site.xml`.

```xml
<property>
    <name>fs.s3a.proxy.host</name>
    <value>{proxy.IP}</value>
</property>
<property>
    <name>fs.s3a.proxy.port</name>
    <value>{proxy.port}</value>
</property>
```

where {proxy.IP} and {proxy.port} are the IP address and port of your proxy.

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

where `{hdfs_dir}` and `{s3_dir}` are the HDFS source directory and S3 target directory.

## Enable S3 support in HDFS（Optional）

This is an optional step. We highly recommend enabling this feature on HDFS for test and trouble shooting. The hadoop version we use is 2.7.3.

### Solve HDFS S3 dependency

First, we need to copy some jar packages about `aws` and `jackson` from `${HADOOP_HOME}/share/hadoop/tools/lib}` to `${HADOOP_HOME}/share/hadoop/common/lib`. The dependencies we need are listed below:

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

## Trouble Shooting

### 1. Performance issue
SSM cold storage feature is based on `s3a feature` of Hadoop. Our module serves as `s3a client` during moving files to s3. Note that default configuration may results poor performance just like any s3a client. If you need higher performance, please enable `s3a fast upload` feature.

You can find more details in these documents: [Hadoop-AWS module: Integration with Amazon Web Services](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html) and [S3A Fast Upload](https://docs.hortonworks.com/HDPDocuments/HDP2/HDP-2.6.3/bk_cloud-data-access/content/s3a-fast-upload.html).

### 2. NoSuchMethodError
Detailed error messages:

```
java.lang.NoSuchMethodError: com.amazonaws.services.s3.transfer.TransferManagerConfiguration.setMultipartUploadThreshold(I)V
at org.apache.hadoop.fs.s3a.S3AFileSystem.initialize(S3AFileSystem.java:285)
at org.apache.hadoop.fs.FileSystem.createFileSystem(FileSystem.java:2669)
at org.apache.hadoop.fs.FileSystem.access$200(FileSystem.java:94)
at org.apache.hadoop.fs.FileSystem$Cache.getInternal(FileSystem.java:2703)
at org.apache.hadoop.fs.FileSystem$Cache.get(FileSystem.java:2685)
at org.apache.hadoop.fs.FileSystem.get(FileSystem.java:373)
at org.smartdata.hdfs.CompatibilityHelper27.getS3outputStream(CompatibilityHelper27.java:166)
at org.smartdata.hdfs.action.Copy2S3Action.copySingleFile(Copy2S3Action.java:130)
at org.smartdata.hdfs.action.Copy2S3Action.execute(Copy2S3Action.java:99)
at org.smartdata.action.SmartAction.run(SmartAction.java:124)
at org.smartdata.server.engine.cmdlet.Cmdlet.runAllActions(Cmdlet.java:94)
at org.smartdata.server.engine.cmdlet.Cmdlet.run(Cmdlet.java:108)
at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511)
at java.util.concurrent.FutureTask.run(FutureTask.java:266)
at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1142)
at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:617)
at java.lang.Thread.run(Thread.java:745)
```
This error only occurs on Hadoop-2.7.3. If you see these messages, please delete `aws-java-sdk-core-1.10.6.jar` and `aws-java-sdk-s3-1.10.6.jar` `${SMART_HOME}/lib`.

## References

1. [S3 Support in Apache Hadoop](https://wiki.apache.org/hadoop/AmazonS3)
2. [Hadoop-AWS module: Integration with Amazon Web Services](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html)
3. [Using the S3A FileSystem Client](https://hortonworks.github.io/hdp-aws/s3-s3aclient/index.html)
4. [S3A Fast Upload](https://docs.hortonworks.com/HDPDocuments/HDP2/HDP-2.6.3/bk_cloud-data-access/content/s3a-fast-upload.html)
