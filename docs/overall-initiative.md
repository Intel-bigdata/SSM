# Smart Data Management Initiative

This initiative introduces the effort to build a storage/data optimization and management framework, with data temperature
in mind. The framework targets for better support and optimization for new storage devices like **NVMe SSD** and **Intel Optane**, high speed
network such as 20Gb and 50Gb NICs, new computing like **[Deep Learning](https://github.com/Intel-bigdata/HDL)**, and even new storage systems, not only HDFS and files, but also other means like cloud and o

**Data temperature** is the first core concept. Because in our view and experience, in a typical deployment the **80%** of computing workloads are often processing the **20%** data in a large data system, however, it is unnecessarily hard to optimize and speed up for all data access and computes, and it is particularly impossible to achieve that in a dynamic and fast evolving environment. That's why we would make endeavor to collect and aggregate data access counts and metrics. Such data will help to figure out and even predict data access patterns, which enables us to optimize data processing accordingly for **HOT** and **WARM** data, particularly benefiting faster storage devices. Meanwhile, this feature also helps a lot in the utilization of large volume of cold storage devices or systems to store amounts of **COLD** data, which alleviates cluster performance degradation.

We consolidate existing facilities in a storage and data system into **actions**, which are runnable and can be dispatched and distributed. Action can be built-in, user-defined or pluggable. We have a unified high level **rule** as our DSL allows admin users to manage and customize their data systems. For example, in a rule you can specify *what data* on *what condition* to execute *what actions*.

**Smart** is another core concept. It does not only mean we build our practical solutions around existing big data storage systems **smartly**, but more importantly it means that we collect and learn from history data and metrics, so the system can become **smart**, and can adjust storage options and data strategies **automatically** and **intelligently** even when no rules given.

That's why **MetaStore** is a core component in the system, where we store data meta and all kinds of collected metrics in history. We leverage a powerful SQL database like MySQL to do so, which allows the system to query and get results instantly about data.

The following lists the storage/data systems that is on-going supported. This work is purely open source and we are open to any ideas and contributions. If you're interested in supporting a system, please PR to update this page and add an entry below.

Supported data systems
---------------------
### 1. Apache HDFS
Apache Hadoop/HDFS is the most widely deployed big data storage system, therefore **HDFS-SSM** support is our major work.

### 2. Alluxio
Alluxio support is also on-going, and China Mobile big data team is working on this aspect.
