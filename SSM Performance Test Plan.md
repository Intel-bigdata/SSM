SSM Performance Test Plan
========


I. Hadoop Configuration
------------------------
Cluster: 1 NameNode + 5 DataNode (Skylake)
Disks: 3 * HDD & 2 * P3700 on each DataNode


II. SSM Configuration
------------------------
1 * Server Node
1 * Database Node
5 * Agents (one on each DataNode)

III. Test Plan
------------------------

### 1. Data Mover Performance Test
#### Measurements:	
* a. Total running time of moving a certain size of data set 
* b. Disk bandwidth data collected by PAT 

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
* b. Small files: 100MB * 1000 files


### 2. Data Sync Performance Test



### 3. Small File Compact Performance Test
* Pending for the code ready 