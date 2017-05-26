# Smart Data Management Initiative

This initiative is an overall high level effort to build a storage/data optimization and management framework with data temperature
in mind. The framework targets for better support and optimization for new storage devices like **NVMe SSD** and **Intel Optane**, high speed
network such as 20Gb and 50Gb NICs, new computing like **Deep Learning**, and even new storage systems, not only for HDFS and files, but also
considering other means like cloud and objects.

At its core **data temperature** is a first citizen concept because in our view, in a typical deployment it's often **80%** computing workloads are
processing the **20%** data in a large data system, it's hard to optimize and speed up for all data access and computings, it's particularly 
impossible to do it in a dynamic and fast evolving environment.

In this framework to support the most widely deployed big data storage system Apache Hadoop and HDFS, **HDFS-SSM** is the major portion of this
effort. Other supports are also on-going. This is purely open source and we are very open to any ideas and contributions from any party.
