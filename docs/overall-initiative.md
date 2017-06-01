# Smart Data Management Initiative

This initiative is an overall high level effort to build a storage/data optimization and management framework with data temperature
in mind. The framework targets for better support and optimization for new storage devices like **NVMe SSD** and **Intel Optane**, high speed
network such as 20Gb and 50Gb NICs, new computing like **[Deep Learning](https://github.com/Intel-bigdata/HDL)**, and even new storage systems, not only for HDFS and files, but also
considering other means like cloud and objects.

At its core **data temperature** is a first citizen concept because in our view and experience, in a typical deployment it's often the **80%** computing workloads are
processing the **20%** data in a large data system, it's unnecessarily hard to optimize and speed up for all data access and computings, it's particularly impossible to do it in a dynamic and fast evolving environment. That's why we would make endeavor to collect and aggregate data access counts and metrics. Such data will help to figure out and even predict data access patterns, which enables us to optimize data processing accordingly for **HOT** and **WARM** data, particularly in favor of faster storage devices. Meanwhile, this also helps a lot to utilize large volume of cold storage devices or systems to store amounts of **COLD** data allieviating cluster performance degradation.

In this framework to support the most widely deployed big data storage system Apache Hadoop and HDFS, **HDFS-SSM** is the major portion of this
effort. Other supports are also on-going. This is purely open source and we are very open to any ideas and contributions from any party.
