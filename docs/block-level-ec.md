Block Level EC 
===============

The default HDFS 3x replication scheme is expensive. It incurs a 200%
storage space overhead and other resources, such as network bandwidth
when writing the data. In Hadoop3.0, HDFS erasure coding feature is
introduced to address this challenge. After a study of the HDFS
file-size distribution, the first phase of HDFS Erasure Coding is to
support EC with striped layout. In this design document, we propose the
contiguous block level EC above SSM framework.

Striped EC vs Block EC
======================

### Striped EC pros and cons

-   Favor small size file

-   No data locality

-   Suitable for code data

EC with striped layout can achieve storage saving with both small files
and large files, especially the small files. While one drawback is it
lost the data locality which may impact the performance of upper layer
application when running on striped EC files. So basically, striped EC
file is more suitable for cold data. For more Striped EC introduction,
refer to this [Cloudera
Blog](https://blog.cloudera.com/blog/2015/09/introduction-to-hdfs-erasure-coding-in-apache-hadoop/).

### Block EC pros and cons

-   Favor large size file

-   Keep data locality

-   Suitable for hot and warn data

Block EC is suitable for very large files because there is space saving
only when full block stripes are written. This contiguous block level EC
is also suitable for offline or background EC. Otherwise a client would
need to buffer GBs of data blocs on memory to calculate parity. Compared
with striped EC, block EC keeps the data locality, it can be used for
persist hot and warm data.

Design Targets 
===============

The following list the targets of this design:

1. Don’t change data write pipeline

2. Transparent data read, on-fly data recovery during read operation if
data is corrupted

2. Transparent data recovery in background

Erasure Coding Policies

This implementation will leverage existing Hadoop 3.0 Erasure Coding
Codecs. All Hadoop 3.0 supported EC policies will be supported on block
level EC. Please refer to [Hadoop 3.0 EC
guide](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HDFSErasureCoding.html)
for detail policy information.

Use Cases
=========

Write Block EC File
-------------------

In SSM infrastructure, all user prefers are represented by rules. For
block EC files, apply the block EC rule to the files. SmartDFSClient
will replace the existing HDFS Client, be responsible to save the data
to HDFS. In the write path, SmartDFSClient will write data into HDFS
first as traditional 3x files, then initiate a task to compute parity
blocks once the file is finalized for write. Append operation to block
EC file will not be supported in the first version. To not impact the
write performance, as long as the parity computation task is created,
the write operation will return as finished, leave the parity
computation task running in background.

<img src="./block-ec-write.png" width="624" height="134" />

Convert 3x Replication file to Block EC file
--------------------------------------------

For some existing 3x replication large files, user many want to convert them to block EC files to save storage space. Apply block EC rule to the files or the directories to trigger the conversion. 

<img src="./block-ec-convert.png" width="624" height="125" />

Read Block EC file 
-------------------

SmartDFSClient will leverage DFSClient to read file content. If there is no data
corrupted, SmartDFSClient will just read data and return the data back
to applications directly.

<img src="./block-ec-read.png" width="481" height="139" />

Read Corrupted Block EC file
----------------------------

If there is data corrupted, SmartDFSClient will then allocate task to
Smart Agent to decode the necessary EC block contents to recover the
lost data, and then return the recovered content to application. Smart
Agent will try to write back the decoded content to data file if
possible.

<img src="./block-ec-read-recovery.png" width="475" height="153" />

Background Reconstruct Corrupted Block EC File
----------------------------

SSM server will also schedule regular tasks to check the integrity of each data file and
parity file. Once content corruption is detected, SSM server will
schedule tasks to Smart Agent to recover the content in background.

<img src="./block-ec-recovery-in-background.png" width="465" height="134" />

Performance Consideration
=========================

In all above five use cases, write file and read file operation have the
same performance as DFSClient. Convert 3x replication file to block EC
file and background reconstruction corrupted file are all transparent to
application. Only the read corrupted file operation will have
performance degradation compared to read 3x replica file. Consider the
massive storage space saving, this trade-off is worthy.

Block EC Meta Data
==================

Block EC file is transparent to HDFS. After user apply the block EC rule
to HDFS files and directories, for each involved HDFS file, there will
be a parity file. SSM server will maintain the relationship between the
HDFS data file and parity file. In SSM meta data store, at least
following information should be recorded for each data file,

-   Data file path

-   EC policy name

-   Parity file path

-   State(data-under-construction, healthy, corrupted,
    parity-under-construction, under-recovery)


