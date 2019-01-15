# HDFS-SSM Design

**Note:** This design doc needs to be updated to reflect latest design change, for example, the adding of SSM agents.

Architecture
------------
![](https://github.com/Intel-bigdata/SSM/blob/trunk/docs/image/architecture.png)

SSM polls metrics from NameNode. These metrics are analyzed by SSM as specified by rules. If all conditions of a certain rule are met, SSM will trigger the corresponding actions.

SSM uses SQL database to maintain the polled data as well as other internal data.

### HA Support
HA is supported. The design will come soon.

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
	* Execute generated actions

## Rules
A rule is an interface between user and SSM, through which the user tells SSM how to function. A rule defines all the things for SSM to work: at what time, to analyze what kind of metrics and conditions, and what actions should be taken when the conditions are met. By writing rules, a user can easily manage their cluster and adjust its behavior for certain purposes.
![](https://github.com/Intel-bigdata/SSM/blob/trunk/docs/image/usage.png)

### Rule Syntax

![](https://github.com/Intel-bigdata/SSM/blob/trunk/docs/image/rule-syntax.png)

### SSM Meta Store
SSM uses a SQL database as MetaStore to maintain data meta info internally. Core tables in SSM:

![](https://github.com/Intel-bigdata/SSM/blob/trunk/docs/image/core-tables.png)

#### Access Count Collection
The below section illustrates how to collect file access counts. As shown in the following chart:

![](https://github.com/Intel-bigdata/SSM/blob/trunk/docs/image/access-count-tables.png)

1. SSM polls accessCount data from NN to get file access count info generated in the time interval (for example, 5s).
2. Create a table to store the info and insert the table name into table access_count_tables.
3. Then file access count of last time interval can be calculated by accumulating data in tables that their start time and end time falls in the interval.
4. AccessCount tables include tables in minute-level, hour-level, day-level, month-level and year-level.To control the total amount of data, accessCount tables in more accurate time level will be aggregated into those more general ones gradually. The longer the time from now, the larger the granularity for aggregation. More accurate data are kept for near now than long ago.
