# HDFS-SSM Design

**Note:** This design doc needs to be updated to reflect latest design change, for example, the adding of SSM agents.

Architecture
------------
![](https://github.com/Intel-bigdata/SSM/blob/trunk/docs/image/architecture.png)

SSM polls metrics from NameNode. These metrics are analyzed by SSM as specified by rules, and if conditions of some rule are fulfilled then it will execute the corresponding actions. 

SSM uses SQL database to maintain data polled as well as other internal data. 

### HA Support
HA is supported. The design will be coming soon.

Desgin
------------
SSM consists of 5 chief components illustrated in the following figure:
![](https://github.com/Intel-bigdata/SSM/blob/trunk/docs/image/design.png)

* StatesManager
	* Collect metrics and events from NameNode
	* Maintain data and forward events 
	
* RuleManager
	* Manage rules
	* Schedule and execute rules
	
* CacheManager
	* Schedule the execution of cache related actions
	
* StorageManager
	* Schedule the execution of storage related actions
	
* ActionExecutor
	* Execute actions generated
	
## Rules
A rule is an interface between user and SSM, through which the user tells SSM how to function. A rule defines all the things for SSM to work: at what time, analysis what kind of metrics and conditions, and what actions should be taken when the conditions are true. By writing rules, a user can easily manage their cluster and adjust its behavior for certain purposes.
![](https://github.com/Intel-bigdata/SSM/blob/trunk/docs/image/usage.png)

### Rule Syntax

![](https://github.com/Intel-bigdata/SSM/blob/trunk/docs/image/rule-syntax.png)

### SSM Meta Store
SSM uses a SQL database as MetaStore to maintain data meta infos internally. Core tables in SSM:

![](https://github.com/Intel-bigdata/SSM/blob/trunk/docs/image/core-tables.png)

#### Access Count Collection
Below is to illustrate how to collect file access counts. As shown in the following chart:

![](https://github.com/Intel-bigdata/SSM/blob/trunk/docs/image/access-count-tables.png)

1. SSM polls accessCount data from NN to get file access count info generated in the time interval (for example, 5s).
2. Create a table to store the info and insert the table name into table access_count_tables.
3. Then file access count of last time interval can be calculated by accumulating data in tables that their start time and end time falls in the interval.
4. To control the total amount of data, second-level of accessCount tables will be aggregated into minute-level, hour-level, day-level, month-level and year-level. The longer the time from now, the larger the granularity for aggregation. More accurate data kept for near now than long ago.
