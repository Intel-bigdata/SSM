HDFS Small Files Solution 
====================

A small file can be defined as a file that is significantly smaller than
the Hadoop block size. Apache Hadoop is designed for handle large files.
It doesn’t works well with lot of small files. There are two primary
impacts if Hadoop has a small file problem, one is NameNode memory
management and another is small file read performance.

There are several existing solutions to handle this small file problem,
such as Hadoop HAR file, sequence file, save small files into HBase etc. For reference, please go to [Cloudera blog](http://blog.cloudera.com/blog/2009/02/the-small-files-problem/).

Most existing solution can resolve NameNode memory management problem
well, but not the read performance problem. We’d like to like to
propose a fully solution to solve the both NameNode memory issue and
read performance issue based on the inspirations from all existing
approaches.

Design Targets 
===============

The following list the targets of this design:

1. Better read performance than current HDFS small file reading

2. At least equivalent if no better small file write performance than current HDFS small file writing

3. Transparent small file read/write from application

4. Optimize NameNode memory usage 


Use Cases
=========

Generally, people are complaining about performance penalty of reading
small files, they are not complaining write small files. Some existing
approach is that there will a routine which compacts all these small
files at some point. We’d like to design our solution from a different
angel. The point is, we want to eliminate the small files at the point
of its writing.

Write small file
----------------

In SSM infrastructure, all user prefers are represented by rules. For
foreseeable small files, apply the small file write rule to the files.
In this case, SmartDFSClient will replace the existing HDFS Client, be
responsible to save the data to HDFS. SmartDFSClient will not direct
create small file in NameNode. Instead, it will query SSM server for
which container file will the small file be saved to, then
SmartDFSClient will talk to SSM agent who is responsible to save the
small file content into the container file.

<img src="./small-file-write.png"/>

Read small file
--------------------------

SSM server has the knowledge about which file is written as special small file.
When access data, SmartDFSClient will first query SSM server to find the corresponding
container file, offset into the container file and length of the
small file, then passes all these information to the Smart Agent to read the data content from the DataNode.

<img src="./small-file-read.png" />


Compact small file
--------------------------

Some application may want to use the exsiting HDFS DFSClient instead of SmartDFSClient while writing datas. The consequence is there can be many small files written into HDFS directly. At some later point, user want to compact all these small files automcailly. To achieve this goal, apply the small file compact rule to the files. With the rule set, SSM server will scan the files and directoirs, schedule tasks to compact small files into big file, and then delete the original small files if preferred. 

<img src="./small-file-compact.png" />

Performance Consideration
=========================

In this solution, we will introduce the concept of container file. A container file is a configurable fixed size HDFS big file, say 1G. A container file will contains hundreds or thousands of small files. By using container file, considerable NameNode memory will be   saved. 

The mapping between small file and container file will be maintained by SSM server. We will also consider use exsintg Hadoop file format, for example, sequence file, as the format of container file.  

The read performance penalty is mainly caused by random diskIO when access many small size files. To improve the small file read
performance, Smart Agent will introduce cache mechanism to cache the whole container block once a piece of content of one small file is read. Most likely, the adjacent small files will be read soon by upper application, so cache ahead will improve the read performance a lot.

By using SmartDFSClient, which provides compatible API definition as existing HDFS DFSClient, small file read and write will be transparent to upper level application.

If user happens to write a big file through the small file write process, SSM can handle this case without obvious performance degrade.

Security Consideration 
=======================

When access small files, SSM server will check whether SmartDFSClient
has the authorization to write to or read from the file. To prevent SmartDFSClient from reading contents execceds its privilege,
all container files will be owned by a special user created for SSM. Only SSM can read and write container file.

Architecture
============

The following diagram shows the small file write and read flow.

<img src="./small-file-write-arch.png" width="481" height="308"/>

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

SSM server manages the meta data storage, which holds the small file to container file mapping information.
Each container file and small file managed by SSM server will have a unique ID, here is a list of small file properties consider to be saved, 
* Name
* ownership, privilege
* Small file ID
* container file ID of this small file
* length
* offset into container file
* create time
* access time
New properties can be added later if required. 

A list of container file properties, more properties can be added later if required. 
* container file ID
* file path in HDFS

Although small files are saved in SSM server, we still provide HDFS compatabile operations, include
* rename small file
* delete small file
* query HDFS file status

Whether "ls" operation supported or not is under investigation. 

The small file read flow path is very similar to write flow path, except the data
content flow direction is different.

<img src="./small-file-read-arch.png" width="481" height="308"/>
