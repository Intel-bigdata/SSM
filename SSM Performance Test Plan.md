SSM Performance Test Plan
========


I. Hadoop Configuration
------------------------
Cluster: 1 NameNode + 5 DataNode (Skylake)
HDFS Disks: 5 * HDD & 2 * P3700 on each DataNode
Shuffle Disks: 2 * SATA SSD 
Block Size: 128MB


II. SSM Configuration
------------------------
1 * Server Node (with Database)
5 * Agents (one on each DataNode)

III. Performance Tests of SSM Core Features
------------------------
This part tests the performance of SSM itself to evaluate the efficiency of SSM running different kinds of tasks.

### 1. Data Mover
#### Purpose
* Test the execution speed of Data Mover.

#### Measurements
* a. Total execution time of moving a certain size of data set 
* b. Disk bandwidth data of each node collected by PAT 

#### Comparison	
* HDFS default mover tool

#### Mover pattern	
* a. ALL-HDD -> ALL-SSD (high priority)
* b. ALL-HDD -> ONE-SSD (high priority)
* c. ALL-SSD -> ALL-HDD (high priority)
* d. ONE-SSD -> ALL-HDD (low priority)
* e. ONE-SSD -> ALL-SSD (low priority)
* f. ALL-SSD -> ONE-SSD (low priority)

#### Data set	
* a. Large files: 10GB * 50 files
* b. Small files: 100MB * 5000 files


### 2. Data Sync

#### Purpose
* Test the execution speed of Data Sync.

#### Target Cluster
* 1 NameNode + 3 DataNode 
* Disks: 2 HDD on each DataNode

#### Measurements
* Execution time of sync a data set.

#### Comparison
* HDFS Distcp tool

#### Data set
* a. Single file:
	* 100MB
	* 1GB
	* 10GB
* b. Directory:
	* 


### 3. Small File Compact 
#### Purpose
* Test the execution speed of compact.


IV. Performance Test of Using SSM in Hadoop
---------------------
This part tests the effect after using different SSM features in order to evaluate the advatage brought by SSM. the benchmarks may include micro benchmarks such as TeraSort and other typical benchmarks in the industry such as TPC-DS, TPC-BB. 

### 1. Data Mover


### 2. Data Sync

### 3. Small File Compact