# SSM Small file User Guide

## Configure small file compact in SSM (Optional)

Compact batch size is the number of small files to be compacted per compact action, whose default value is 200.
Default container file threshold size is 1G. User can set prefer values in `${SMART_HOME}/conf/smart-site.conf`.

* Configure compact batch size
  ```xml
  <property>
    <name>smart.compact.batch.size</name>
    <value>200</value>
    <description>The number of small files to be compacted per action.</description>
  </property>

* Configure container file threshold size
  ```xml
  <property>
    <name>smart.compact.container.file.threshold.mb</name>
    <value>1024</value>
    <description>The threshold size of container file, in units of MB.</description>
  </property>

## Small file compact rule example

```
file: path matches "/small_files/*" and length < 1MB | compact
```

This rule means all the files under `/small_files` directory, if the length of file is less than 1MB,
SSM will trigger actions to compact these files to container files.

## Small file compact action example

```
compact –file ['/small_files/1.txt','/small_files/2.txt'] -containerFile /container_file
```

This action means SSM will trigger an action to compact these specified files: `/small_files/1.txt`,
`/small_files/2.txt` to the container file: `/container_file`.

## Small file uncompact action example

```
uncompact -containerFile /container_file
```

This action means SSM will trigger an action to uncompact small files from the container file: `/container_file`.
