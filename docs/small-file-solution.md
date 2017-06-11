Transparent Small Files Support 
====================

A small file can be defined as a file that is significantly smaller than the Hadoop block size. Apache Hadoop is designed for handling large files. It doesn’t works well with lots of small files. There are two primary impacts for HDFS, one is NameNode memory consumption and namespace explosion, the other is small file write/read performance with the introduced overhead comparing with the small content.

There are several existing solutions to handle this small file problem, such as Hadoop HAR file, sequence file, saving small files into HBase etc. A good read about this is [HDFS small files problem](http://blog.cloudera.com/blog/2009/02/the-small-files-problem/). Additionally, there was an attempt to solve this problem in [HDFS-8998](https://issues.apache.org/jira/browse/HDFS-8998). A large handle related to small files problem on-going in Apache Hadoop community is [OZone effort](https://issues.apache.org/jira/browse/HDFS-7240).

Most existing solutions may solve some of the problems well, but maybe not transparently to applications, or introducing non-trivial modification into HDFS. We’d like to propose a solution to solve these HDFS small files problems in the framework of SSM on top of HDFS based on the ideas from existing approaches and discussions with industry experts. 

In this solution, we introduce a concept of container file. A container file is a normal big HDFS file with configurable threshold size, say 1G. A container file can contains hundreds or thousands of small files. The mapping between small files and container file are maintained by SSM metastore. As the format of container file, we may consider existing Hadoop file format. SSM metastore holds the mapping information between small files and container file. The mapping maintains small file id, container file id and index info.

Design Targets 
===============

The following list the targets of this design:

1. Better read performance than current HDFS small file read in average.

2. At least equivalent if no better small file write performance than current HDFS small file write.

3. Optimize NameNode memory usage and compact namespace.

4. Transparent small file read/write for applications.

Use Cases
=========

We want to optimize and solve the small files problem in 3 cases, not only for read, but also for write. For existing small files in an HDFS cluster, we also support compaction.

### 1. Write new small file

In SSM infrastructure, all user preferences are represented by rules. For foreseeable small files, apply the small file write rule to the files. In this case, SmartDFSClient will replace the existing HDFS Client, be responsible to save the data to HDFS. SmartDFSClient will not directly create small file in NameNode. Instead, it will query SSM server for which container file (a normal HDFS file used for small fiels bucket) will the small file be saved to, then SmartDFSClient will talk to SSM agent who is responsible to save the small file content into the container file.

<img src="./small-file-write.png"/>

### 2. Read small file

To read a small file, SSM server has the knowledge about which container file the small file is stored into, when read the small data, SmartDFSClient will first query SSM server to find the corresponding container file, offset into the container file and length of the small file, then passes all these information to the Smart Agent to read the data content from the DataNode.

<img src="./small-file-read.png" />

### 3. Compact existing small files

There can be many small files written into HDFS already in an existing deployment and users may want to compact all these small files. To achieve this goal, apply the small file compact rule to the files. With the rule set, SSM server will scan the files and directories, schedule tasks to compact small files into big container file, and then delete the original small files. 

<img src="./small-file-compact.png" />

Architecture
============

The following diagram shows the small file write flow.

<img src="./small-file-write-arch.png" width="550" height="350"/>

Here is the writing flow,

1.  SmartDFSClient first communicate with SSM server once it wants to
    create a new file. SSM server will check if the file goes to
    directory which has small-file-write rule applied. If it is a small
    file, SSM server will do privilege check to guarantee that
    SmartDFSClient has the privilege to write data into the directory.
    If the privilege check fails, SSM server will return error
    to SmartDFSClient.

2.  After the privilege check is passed. SSM server queriess
    metadata store about which container file is suitable to hold the
    new small file, and from which offset of the container file to start put
    the new content, also which SSM Agent will be the proxy to chain the
    data writing action. SSM server then packages all these information
    into a token, and return the token to SmartDFSClient.

3.  SmartDFSClient passes the token received from SSM server,
    together with the file content to the corresponding SSM Agent.

4.  SSM Agent is responsbile to write the content of small file into the container
    file effectively.

If user happens to write a big file through the small file write process, SSM can handle this case without obvious performance degrade.

The small file read flow path is very similar to write flow path, except the data content flow direction is different.

<img src="./small-file-read-arch.png"  width="550" height="350"/>

In additon to write and read, we also provide HDFS compatabile operations as follows. Note all these operations will be done against SSM metastore instead of NameNode since small files meta are kept in the metastore. We don't support append and truncate small files, we can consider such later in future.
* rename small file
* delete small file
* query small file status
* list small files

Performance Consideration
=========================

The read performance penalty is mainly caused by random diskIO when access many small size files. To improve the small file read
performance, Smart Agent may leverage HDFS cache mechanism to cache the whole container block once a small file content in the block is read. Most likely, the adjacent small files will be read soon by upper application, so cache ahead or read ahead may improve the read performance a lot. Generally container files will be regarded as **HOT** so policy like ALL-SSD can be used to speed up the read.

A special case is small files batch read. In such case new APIs will be provided by SmartDFSClient allowing applications to list small files buckets/containers and read batch of small files in a container/bucket at a time. Some new Hadoop format based on this can also be considered. This case is particularly useful in deep learning training, because many small image files are fed to a training worker as a batch at a time. 

Security Consideration 
=======================

When read small files, SSM server will check whether the user has the necessary permission. Container files are owned by SSM only and only SSM (agent) can see/write/read them. End user or SmartDFSClient can't write to/read from container files directly.
