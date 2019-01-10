Transparent Small Files Support 
====================

A small file refers to a file that is significantly smaller than the Hadoop block size. Apache Hadoop is designed for handling large files. It does not work well with lots of small files. There are primary two kinds of impacts for HDFS. One is related to NameNode memory consumption and namespace explosion, while the other is related to small file write/read performance with the introduced overhead compared with the small content.

There are several existing solutions to handle the small file problem, including Hadoop HAR file, sequence file and saving small files into HBase etc.  This document [HDFS small files problem](http://blog.cloudera.com/blog/2009/02/the-small-files-problem/) is worthy of reference. Additionally, there was an attempt to solve this problem in [HDFS-8998](https://issues.apache.org/jira/browse/HDFS-8998). Another detailed description related to the small files problem is [OZone effort](https://issues.apache.org/jira/browse/HDFS-7240) in Apache Hadoop community.

Most existing solutions may solve part of the problems well, but may be not transparent to applications, or introduce non-trivial modification into HDFS. We’d like to propose a solution to solve these HDFS small files problems in the framework of SSM on top of HDFS, based on the ideas learned from existing approaches and industry experts.

In this solution, we introduce the concept of container file. A container file is a normal big HDFS file with configurable threshold size, saying 1G, which can contain hundreds or thousands of small files. The mapping between small files and container file is maintained by SSM metastore. As for the format of container file, we may consider existing Hadoop file format. SSM metastore holds the mapping information between small files and container file. The mapping maintains small file id, container file id and index info.

Design Targets 
===============

The following lists the targets of this design:

1. Better read performance than current average read performance of HDFS small file.

2. At least equivalent with if not better than current HDFS small file write performance.

3. Optimized NameNode memory usage and compact namespace.

4. Transparent small file read/write for applications.

Use Cases
=========

We want to optimize and solve the small files problem in 3 cases, for both read and write. We also support compaction for existing small files in an HDFS cluster.

### 1. Write new small file

In SSM framework, all user preferences are represented by rules. For foreseeable small files, the small file write rule is applied to the files. In this case, SmartDFSClient will replace the existing HDFS Client to be responsible for saving the data to HDFS. SmartDFSClient will not directly create small file in NameNode. Instead, it will query SSM server to make sure which container file (a normal HDFS file used as small files bucket) the small file will be saved into, then SmartDFSClient will talk to SSM agent about who is responsible to save the small file content into the container file.

<img src="./image/small-file-write.png"/>

### 2. Read small file

To read a small file, SSM server has the knowledge of which container file the small file is stored in. When reading the small file, SmartDFSClient will firstly query SSM server to acquire the information including corresponding container file, offset into the container file and length of the small file, then it will transfer all these information to the Smart Agent to read the data content from the DataNode.

<img src="./image/small-file-read.png" />

### 3. Compact existing small files

There can be many small files written into HDFS already in an existing deployment and users may want to compact all these small files. To achieve this goal, the small file compact rule is applied to the files. With the rule set, SSM server will scan the directories and files, schedule tasks to compact small files into big container file, and then truncate the original small files.

<img src="./image/small-file-compact.png" />

Architecture
============

The following diagram shows the small file write flow.

<img src="./image/small-file-write-arch.png" width="550" height="350"/>

The following is the description of the writing flow.

1.  SmartDFSClient firstly communicate with SSM server when
    creating a new file. SSM server will check if the file goes to
    directory applied with small-file-write rule. If it is a small
    file, SSM server will execute privilege check to guarantee that
    SmartDFSClient has the privilege to write data into the directory.
    If the privilege check fails, SSM server will return error
    to SmartDFSClient.

2.  After the privilege check is passed, SSM server will query
    metadata store to acquire information including the suitable container file to hold the
    new small file, the offset of the container file from which to start put
    the new content, and the SSM Agent to be the proxy to chain the
    data writing action. SSM server then packages all these information
    into a token, and return the token to SmartDFSClient.

3.  SmartDFSClient transfers the token received from SSM server and the file content to the corresponding SSM Agent.

4.  SSM Agent is responsible for writing the content of small file into the container
    file effectively.

If user happens to write a big file through the small file write process, SSM can handle this case without obvious performance degrade.

The small file read flow path is very similar to write flow path, except different data flow direction.

<img src="./image/small-file-read-arch.png" width="550" height="350"/>

Other HDFS operations support
=============================

### 1. Supported operations

In addition to write and read, we also provide many HDFS compatible operations. Some of the operations do not need to get any information from SSM server, while others may need to get file container info from meta store first or require special handling.

i. Now that the original small files are truncated after compact, the meta data are still preserved in the namespace. Below shows the operations which need to get information from namespace.

* Get and set extended attributes: getXAttr, getXAttrs, listXAttrs, setXAttr, removeXAttr.
* Get and check acl info: getAclStatus, checkAccess.
* Get and set some other meta data: getBlockSize, exists, listPaths, setTimes.

ii. For the file container info (corresponding container file, offset and length) of small files are stored in SSM meta store, some operations need to firstly query SSM server to get the file container info, then use these information to send exact requests to HDFS server.

* Get block info: getLocatedBlocks, getBlockLocations, getFileBlockLocations.
* Get file info: getFileInfo, listStatus, listStatusIterator, getFileStatus, isFileClosed.
  
iii. The following operations impact small files' meta in namespace as well as meta store of SSM.

* Rename small file: rename small file in both namespace and meta store.

* Delete small file: delete small file in hdfs, then delete the file container info of small file in meta store.

* Truncate small file: since the small file is already truncated in hdfs, the length of small file is set to zero in meta store.

> Note that the content of small file is still stored in the container file after delete or truncate.

### 2. Unsupported operations

There are a number of operations, such as setAcl, which are not supported at present, but we can consider their supports in the future.

i. Operations below can be executed successfully, but the results are not accurate.

* Get and set storage policy: getStoragePolicies, setStoragePolicy.
* Get and set cache: addCacheDirective, removeCacheDirective, listCacheDirectives, modifyCacheDirective.
* Others: setReplication, getContentSummary.

ii. Operations below are not allowed to execute.

* Set acl: setPermission, setOwner, modifyAclEntries, removeAclEntries, setAcl, removeDefaultAcl.
* Symlink: createSymlink, getFileLinkStatus, getLinkTarget, getFileLinkInfo.
* Get checkSum: getFileChecksum.
* Others: concat, listCorruptFileBlocks.

Performance Consideration
=========================

The read performance penalty is mainly caused by random diskIO when access small-size files. To improve the small file read
performance, Smart Agent may leverage HDFS cache mechanism to cache the whole container block once a small file content in the block is read. Most likely, the adjacent small files will be read soon by upper application, so cache ahead or read ahead may improve the read performance a lot. Generally container files will be regarded as **HOT** so policy like ALL-SSD can be used to speed up the read.

A special case is small files batch read. In such case new APIs will be provided by SmartDFSClient, allowing applications to list small files buckets/containers and to read batch of small files in a container/bucket at a time. Some new Hadoop format based on this feature can also be considered. This case is particularly useful in deep learning training, because many small image files are fed to a training worker as a batch at a time.

Security Consideration 
=======================

For now container file includes the small files which have the same acl under a folder, and the container file is saved in the same directory as small files. In this way we can ensure container file has the same acl as small files.

When reading small file, SSM server will check whether the user has the necessary permission to the small file, and after privilege check passed, SSM server will use the file container info queried from meta store to read small file from the container file.
