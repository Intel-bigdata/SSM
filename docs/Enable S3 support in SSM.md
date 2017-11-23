# Enable S3 support in SSM

SSM has already supported S3 in copy action. Now we can set a remote S3 path as a destination in 'copy2s3' action.

## How to enable S3 support in HDFS（optional）

This is an optional step. To test S3 support of SSM, we can use the command of HDFS to check the file in remote S3 cluster(AWS S3). The hadoop version is 2.7.

### Solve the dependency

It is easy enable S3 support of HDFS. First we need to copy some jar packages about `aws` and `jackson` from `${HADOOP_HOME}/share/hadoop/tools/lib}` to `${HADOOP_HOME}/share/hadoop/tools/common/lib`. 

The jar dependencies we need to copy:

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

We can find these jar dependencies in `${HADOOP_HOME}/share/hadoop/tools/lib}` by using commands like this:

```shell
ll | grep jackson
ll | grep aws
```

### Add configuration of AWS S3

Then we add some configurations in `${HADOOP_HOME}/etc/core-site.xml`:

```xml
<property>
        <name>fs.s3a.access.key</name>
        <value></value>
</property>
<property>
        <name>fs.s3a.secret.key</name>
        <value></value>
</property>
```

### Try to use HDFS command

We can use the command `hdfs dfs -ls s3a://{YOU_PATH}/` to list the file in remote AWS S3 cluster.

An example:

```shell
$ hdfs dfs -ls s3a://xxxctest/
Found 2 items
-rw-rw-rw-   1       2048 2017-11-19 23:47 s3a://xxxctest/1.txt
-rw-rw-rw-   1          1 2017-11-21 18:58 s3a://xxxctest/1511319531258
```



## Copy file from SSM to S3

### Add configuration in SSM

SSM has already solve the dependency of S3. We only need to add some configuration in `${SSM_HOME}/conf/smart-site.xml`:

```xml
<property>
        <name>fs.s3a.access.key</name>
        <value></value>
</property>
<property>
        <name>fs.s3a.secret.key</name>
        <value></value>
</property>
```

### The action format

We can use the following action to copy file from SSM to AWS S3:

```shell
copy2s3 {src} {dest}
```

This is a example in SSM WebUI command:

```shell
copy2s3 /test/copytest s3a://xxxctest/
```

### Use rule to trigger the action

```shell
file: path matches "/test/*" and age > 30d | copy2s3 /test/copytest s3a://xxxctest/
```



