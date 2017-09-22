Deployment SSM with CDH5.10 Guide
----------------------------------------------------------------------------------
Requirements:

* Unix System
* JDK 1.7
* CDH 5.10.1
* MySQL Community 5.7.18
* Maven 3.1.1+


Why JDK 1.7 is preferred
----------------------------------------------------------------------------------

  It is because CDH5.10.1 by default support compile and run with JDK 1.7. If you
  want to use JDK1.8, please turn to Cloudera web site for how to support JDK1.8
  in CDH5.10.1.
  For SSM, JDK 1.7 and 1.8 are both supported.

SSM Configuration
---------------------------------------------------------------------------------
1. Download SSM branch from Github https://github.com/Intel-bigdata/SSM/ 
2. Build SSM using 

   'mvn package -Pdist,web,hadoop-cdh-2.6 -DskipTests'

   A tar distribution package will be generated under 'smart-dist/target'.
   More detail information, please refer to BUILDING.txt file. Unzip the distribution package to directory (assuming `${SMART_HOME}`) and switch to the directory, the configuration files are under './conf'. 

3. Configure How to acces Hadoop Namenode

   We need to let SSM know where Hadoop Namenode is. There are 2 cases,
   
   a.  HA Namenode
   
   open smart-site.xml, configure Hadoop cluster NameNode RPC address, fill the value field with Hadoop configuration files path, for exmaple "file:///etc/hadoop/conf".
   
   ```xml   
   <property>
       <name>smart.hadoop.configuration.path</name>
       <value>/conf</value>
       <description>local file path which holds all hadoop configuration files, such as hdfs-site.xml, core-site.xml</description>
    </property>
   ``` 
   
   b.  Single Namenode
   
   open smart-site.xml, configure Hadoop cluster NameNode RPC address,
   
   ```xml
   <property>
   <name>smart.dfs.namenode.rpcserver</name>
      <value>hdfs://namenode-ip:9000</value>
      <description>Hadoop cluster Namenode RPC server address and port</description>
   </property>
   ```
   
4. Configure how to access MySQL DB

   You need to install an Mysql instance first. Then open conf/druid.xml, configure how SSM can access MySQL DB. Basically fill out the
   DB url, username and password are enough. Please be noted that, security support will be enabled later. Here is an example, 
   
   ```xml
   <properties>
       <entry key="url">jdbc:mysql://localhost/ssm</entry>
       <entry key="username">root</entry>
       <entry key="password">123456</entry>
	   ......
   </properties>	   
   ```
   
   `ssm` is the Database name. 

5. Format Database
	
	`bin/init-ssm.sh`
   
   The script will create all tables required by SSM.
	
   
6. Start SSM server
   
   SSM server requires HDFS superuser privielge to access some Namenode APIs. So please make sure the account you used to start SSM has the provilege. 
   
   The start process also requires the user account to start the SSM server is able to ssh to localhost without providing password.  

   `./bin/start-ssm.sh" --config ./conf`

   `--config <config-dir>` configures where the configuration file directory is. `${SMART_HOME}/conf` is used if not specified.
   
   Once you start the SSM server, you can open its web UI by 

   `http://localhost:7045`

   If you meet any problem, please open the SmartServer.log under /logs directory. All the
   trouble shooting clues are there. 
   
7. Stop SSM server
   If stop SSM server is required, use the script `bin/stop-ssm.sh`. Remeber to
   use the exact same parameters to `stop-ssm.sh` script as to `start-ssm.sh` script.
   For exmaple, when you start smart server, you use 

   `bin/start-ssm.sh --config ./conf`

   Then, when you stop smart server, you should use 

   `bin/stop-ssm.sh --config ./conf`


CDH 5.10.1 Configuration
----------------------------------------------------------------------------------
After install CDH5.10.1, please do the following configurations, 

1. Hadoop `core-site.xml`

Change property `fs.hdfs.impl` value, to point to the Smart Server provided "Smart
File System".

```xml
<property>
  <name>fs.hdfs.impl</name>
  <value>org.smartdata.hadoop.filesystem.SmartFileSystem</value>
  <description>The FileSystem for hdfs URL</description>
</property>
```

2. Hadoop `hdfs-site.xml`

Add property `smart.server.rpc.adddress` and `smart.server.rpc.port` to point to 
installed Smart Server.'

```xml
<property>
  <name>smart.server.rpc.address</name>
  <value>ssm-server-ip</value>
</property>
<property>
  <name>smart.server.rpc.port</name>
  <value>7042</value>
</property> 
```

Make sure you have the correct HDFS storage type applied to HDFS DataNode storage
volumes, here is an example which set the SSD, DISK and Archive volumes,

```xml
<property>
 <name>dfs.datanode.data.dir</name>
 <value>[SSD]file:///Users/drankye/workspace/tmp/disk_a,[DISK]file:///Users/drankye/workspace/tmp/disk_b,[ARCHIVE]file:///Users/drankye/workspace/tmp/disk_c</value>
</property>
```

3. Make sure CDH can access SSM jars

After we switch to the SmartFileSystem from the default HDFS implmentation, we need to make sure CDH can access SmartFileSystem
implementation jars, so that HDFS, YARN and other upper layer applications can access. Basically
when SSM compilation is finished, copy all the jar files starts with smart under 

`/smart-dist/target/smart-data-0.1-SNAPSHOT/smart-data-0.1-SNAPSHOT/lib`

to directory in CDH class path, or add the above path to CDH classpath. 

After all the steps, A cluster restart is required. After the restart, try to run some simple test to see if 
the configuration takes effect. You can try TestDFSIO for example, 

 	a. write data
 
	`hadoop jar ./share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-2.6.0-cdh5.10.1-tests.jar TestDFSIO –write –nrFiles 5 –size 5MB`
   
 	b. read data

	`hadoop jar ./share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-2.6.0-cdh5.10.1-tests.jar TestDFSIO –read`

   You may want to replace the jar with the version used in your CDH. After the read data opertion, if all the data files are listed on SSM web UI page "hot files" table, then the integration works very well. 


SSM Rule Examples
---------------------------------------------------------------------------------
1. Move to SSD rule

	`file: path matches "/test/*" and accessCount(5m) > 3 | allssd`

This rule means all the files under /test directory, if it is accessed 3 times during
last 5 minutes, SSM should trigger an anction to move the file to SSD. Rule engine
will evalue the condition every MAX{5s,5m/20} internal.


2. Move to Archive(Cold) rule

	`file: path matches "/test/*" and age > 5h | archive`

This rule means all the files under /test directory, if it's age is more than 5 hours,
then move the file to archive storage.

3. Move one type of file to specific storage

	`file: path matches "/test/*.xml" | allssd`

This rule will move all XML files under /test directory to SSD. In this rule, neither a
single date nor time value is specified, the rule will be evaluated every short time interval (5s by default).

4. Specify rule evaluation internal

	`file: every 3s | path matches "/test/*.xml" | allssd`
  
This rule will move all XML files under /test directory to SSD. The rule engine will
evaluate whether the condition meets every 3s. 

Rule priority and rule order will be considered to implement yet. Currenlty all rules
will run parallelly. For a full detail rule format definition, please refer to 
https://github.com/Intel-bigdata/SSM/blob/trunk/docs/admin-user-guide.md


Performance Tunning
---------------------------------------------------------------------------------
There are two configurable parameters which impact the SSM rule evalute and action 
execution parallism.

1. smart.rule.executors

Current default value is 5, which means system will concurrently evalue 5 rule state 
at the same time.

2. smart.cmdlet.executors

Current default value is 10, means there will be 10 actions concurrenlty executed at
the same time. 
If the current configuration cannot meet your performance requirements, you can change
it by define the property in the smart-site.xml under /conf directory. Here is an example
to change the ation execution paralliem to 50.

```xml
<property>
  <name>smart.cmdlet.executors</name>
  <value>50</value>
</property>
```

SSM service restart is required after the configuration change. 


Trouble Shooting
---------------------------------------------------------------------------------
All logs will go to SmartSerer.log under /logs directory. 

1. Smart Server can't start successfully

   a. Check whether Hadoop HDFS NameNode is running
   
   b. Check whether MySQL server is running
   
   c. Check if there is already a SmartDaemon process running
   
   d. Got to logs under /logs directory, find any useful clues in the log file.
   
2. UI can not show hot files list 

  Possible causes:
  
  a. Cannot lock system mover locker. You may see something like this in the SmartServer.log file,
  
	2017-07-15 00:38:28,619 INFO org.smartdata.hdfs.HdfsStatesUpdateService.init 68: Initializing ...
	2017-07-15 00:38:29,350 ERROR org.smartdata.hdfs.HdfsStatesUpdateService.checkAndMarkRunning 138: Unable to lock 'mover', please stop 'mover' first.
	2017-07-15 00:38:29,350 INFO org.smartdata.server.engine.StatesManager.initStatesUpdaterService 180: Failed to create states updater service.
	 
  Make sure there is no system mover running. Try to restart the SSM service will solve the problem. 
	 
	 
Know Issues(2017-08-19)
---------------------------------------------------------------------------------
1. On UI, actions in waiting list will show "CreateTime" 1969-12-31 16:00:00 and "Running Time" 36 weeks and 2 days. This will be improved later. 
2. If there is no SSD and Archive type disk volumn configured in DataNodes, action generated by "allssd" and "archive" rule wil fail.
3. When SSM starts, it will pull the whole namespace form Namenode. If the namespace is very big, it will takes minutes for SSM to finish the namespace synchronization. SSM may half fuction during this period. 


   
