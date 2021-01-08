# SSM Compression Feature User Guide

## Compression/Decompression Action
Compression action. This action is used to compress a given file. The below codec and bufSize are optional to be specified.
```
compress –file $path [-codec $codec -bufSize $size]
```

Decompression action. This action is used to decompress a given compressed file.
```
decompress -file $path [-bufSize $size]
```

Check compression action. This action is used to check the compression status for a given file.
```
checkcompress -file $path
```

## Compression/Decompression Rule

Example:
```
file: path matches "/compress/*" | compress -codec snappy
```
The above rule means for all files under `/compress` directory, SSM will trigger actions to compress them with snappy. If new files are added to this directory, SSM will also trigger actions to compress these new files.

```
file: path matches "/decompress/*" | decompress
```
The above rule is used to decompress all compressed files under `/decompress`.

## Configure Compression in SSM (Optional)

Default codec is Zlib (if not given in action or rule), user can set other codec in `${SMART_HOME}/conf/smart-site.xml`.

An example for configuring default codec:

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

* SSM will load Hadoop native lib for supporting some native compression codecs, such as Lz4, Bzip2, snappy. To load the lib, user can set LD_LIBRARY_PATH in conf/smart-env.sh or global environment,
e.g., export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/root/hadoop/lib/native/. Otherwise, SSM will fail to load Hadoop native lib and only built-in Zlib can be used.

* Appending data to compressed file is not supported.

* After data is compressed by SSM, user needs to use SmartDFSClient to get the original data instead of DFSClient which returns the raw compressed data. Please see Hadoop Configuration part in ssm-deployment-guide.md
for replacing DFSClient by SmartDFSClient in Hadoop. SmartDFSClient has overridden DFSClient's getFileInfo method in order to return the original file's info, for example original length to user. Thus, user can see
original length of compressed file by using `hdfs dfs -ls`.

* It is supported to sync or copy compressed data to another cluster. But, the data is firstly decompressed and then transferred to the given cluster, which means SSM compression cannot be used to reduce network IO load
in syncing data. Besides, the backup file will not be compressed.

* SSM will write a few metadata into HDFS file xattr after compression. The required xattr size depends on the size of file to be compressed. For large file, HDFS max xattr size may be exceeded, which can be resolved by
setting a larger value for `dfs.namenode.fs-limits.max-xattr-size` in HDFS.
