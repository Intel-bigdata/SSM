# Smart Data Management Initiative

This initiative is an overall high level effort to build a storage/data optimization and management framework with data temperature
in mind. The framework targets for better support and optimization for new storage devices like **NVMe SSD** and **Intel Optane**, high speed
network such as 20Gb and 50Gb NICs, new computing like **[Deep Learning](https://github.com/Intel-bigdata/HDL)**, and even new storage systems, not only for HDFS and files, but also
considering other means like cloud and objects.

At its core **data temperature** is a first citizen concept because in our view and experience, in a typical deployment it's often the **80%** computing workloads are processing the **20%** data in a large data system, it's unnecessarily hard to optimize and speed up for all data access and computings, it's particularly impossible to do it in a dynamic and fast evolving environment. That's why we would make endeavor to collect and aggregate data access counts and metrics. Such data will help to figure out and even predict data access patterns, which enables us to optimize data processing accordingly for **HOT** and **WARM** data, particularly in favor of faster storage devices. Meanwhile, this also helps a lot to utilize large volume of cold storage devices or systems to store amounts of **COLD** data allieviating cluster performance degradation.

We consolidate existing facilities in a storage and data system into **actions**, which are runnables, can be dispatched and distributed. Action can be built-in and also user defined or pluggable. We have a unified high level **rule** as our DSL allowing admin users to manage and customize their data systems. In a rule for example, you can specify for *what data* on *what condition* do *what actions*.

**Smart** is another core concept. It does not only mean we build our pratical solutions around existing big data storage systems **smartly**, but more important also mean we collect and learn from history data and metrics, so the system can become **smart**, and can adjust storage options and data strategies **automatically** and **intelligently** even when you given no rules.

That's why **MetaStore** is a core component in the system, where we store data meta and all kinds of collected metrics in history. We leverage a powerful SQL database like MySQL to do so, which allows the system to query and get results instantly about data.

The following lists the storage/data systems that's on-going supported. This is purely open source and we are very open to any ideas and contributions from any party. If you're interested in supporting a system, please PR to update this page and add an entry below.

Supported data systems
---------------------
### 1. Apache HDFS
Apache Hadoop/HDFS is the most widely deployed big data storage system, **HDFS-SSM** is our major work to support it in this effort. 
