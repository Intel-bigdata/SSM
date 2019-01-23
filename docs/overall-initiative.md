# Smart Data Management Initiative

This initiative introduces the effort to build a storage/data optimization and management framework, with data temperature
in mind. The framework targets for better support and optimization for new storage devices like **NVMe SSD** and **Intel Optane**, high speed
network such as 20Gb and 50Gb NICs, new computing like **[Deep Learning](https://github.com/Intel-bigdata/HDL)**, and even new storage systems, not only HDFS and files, but also other means like cloud and objects.

**Data temperature** is the first core concept. Because in our view and experience, in a typical deployment the **80%** of computing workloads are often processing the **20%** data in a large data system, so it is unnecessary to optimize for all data. It is also particularly impossible to achieve that in a dynamic and fast evolving environment. That's why we make endeavor to collect and aggregate data access counts and metrics. Such data will help to figure out and even predict data access patterns, which enables us to optimize data processing accordingly for **HOT** and **WARM** data, particularly benefiting from faster storage devices. Meanwhile, this feature also helps a lot in the utilization of large volume of cold storage devices or systems to store amounts of **COLD** data, which alleviates cluster performance degradation.

We consolidate existing facilities in a storage and data system into **actions**, which are runnable and can be dispatched and executed. Action can be built-in, user-defined or pluggable. We have a unified high level **rule** as our DSL allows admin users to manage and customize the operations on their data. For example, in a rule you can specify *what data* on *what condition* to execute *what actions*.

**Smart** is another core concept. It does not only mean we build our practical solutions around existing big data storage systems **smartly**, but more importantly it means that we collect and learn from history data and metrics, so the system can become **smart**, and can adjust storage options and data strategies **automatically** and **intelligently** even when no rule is given.

**MetaStore** is a core component in the system to store meta data and collected historical metrics. A SQL database, especially MySQL, can be used to serve as metastore.

The following lists the storage/data systems that is on-going supported. This work is purely open source and we are open to any ideas and contributions. If you're interested in supporting a system, please PR to update this page and add an entry below.

Supported data systems
---------------------
### 1. Apache HDFS
Apache Hadoop/HDFS is the most widely deployed big data storage system, therefore **HDFS-SSM** support is our major work.

### 2. Alluxio
Alluxio support is also on-going, and China Mobile big data team is working on this aspect.
