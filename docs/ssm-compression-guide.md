# SSM Compression User Guide

## Usage
Basic usage
```
compress –file [file path] -codec [codec]
```

Optional parameters
```
-bufSize [buffer size]
```

## Compression action example

```
compress –file /compress/1.txt -codec snappy
```

This action means SSM will trigger an action to compress these specified file, i.e., `/compress/1.txt`. The original file will be replaced with compressed file. The compression codec is snappy.

## Compression rule example

```
file: path matches "/compress/*" | compress -codec snappy
```

This rule means for all files under `/compress` directory, SSM will trigger actions to compress them with snappy. If new files are added to this directory, SSM will also trigger actions to compress these new files.

## Configure Compression in SSM (Optional)

Default codec is Zlib (if not given in action or rule), user can set other codec in `${SMART_HOME}/conf/smart-site.xml`.
* Configure default codec
  ```xml
  <property>
    <name>smart.compression.codec</name>
    <value>Snappy</value>
    <description>
      The default compression codec for SSM compression (Zlib, Lz4, Bzip2, snappy).
      User can also specify a codec in action arg, then this default setting will
      be overridden.
    </description>
  </property>
  ```

## Note

SSM will load Hadoop native lib for supporting some native compression codecs, such as Lz4, Bzip2, snappy. To load the lib, $HADOOP_HOME or $HADOOP_COMMON_HOME should be configured in your environment. Otherwise, only built-in Zlib can be used.

Appending data to compressed file is not supported.

After data is compressed by SSM, user needs to use SmartDFSClient to get the original data instead of DFSClient which returns the raw compressed data. Please see Hadoop Configuration part in ssm-deployment-guide.md
for replacing DFSClient by SmartDFSClient in Hadoop. SmartDFSClient has overridden DFSClient's getFileInfo method in order to return the original file's info, for example original length to user. Thus, user can see
original length of compressed file by using `hdfs dfs -ls`.

It is supported to sync or copy compressed data to another cluster. But, the data is firstly uncompressed and then transferred to the given cluster, which means SSM compression cannot be used to reduce network IO load
in syncing data. Besides, the backup file will not be compressed.