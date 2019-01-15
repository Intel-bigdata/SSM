# SSM Deployment Guide with Hadoop
----------------------------------------------------------------------------------
## Requirements:

* Unix/Unix-like OS
* JDK 1.7 for CDH 5.10.1 or JDK 1.8 for Apache Hadoop 2.7.3/3.1.0
* CDH 5.10.1 or Apache Hadoop 2.7.3/3.1.0
* MySQL Community 5.7.18+
* Maven 3.1.1+ (merely for build use)

## Why JDK 1.7 is preferred for CDH 5.10.1

  It is because by default CDH 5.10.1 supports compile and run with JDK 1.7. If you
  want to use JDK1.8, please turn to Cloudera web site for how to support JDK1.8
  in CDH 5.10.1.
  For SSM, JDK 1.7 and 1.8 are both supported.


# Build SSM Package
---------------------------------------------------------------------------------
##  **Download SSM**

Download SSM branch from Github https://github.com/Intel-bigdata/SSM/ 

##  **Build SSM**

###   For CDH 5.10.1
  
  	mvn clean package -Pdist,web,hadoop-cdh-2.6 -DskipTests
   
###   For Hadoop 2.7.3
  	
	mvn clean package -Pdist,web,hadoop-2.7 -DskipTests

###   For Hadoop 3.1.0

	mvn clean package -Pdist,web,hadoop-3.1 -DskipTests

A tar distribution package will be generated under 'smart-dist/target'. unzip the tar distribution package to ${SMART_HOME} directory, the configuration files of SSM is under '${SMART_HOME}/conf'.
More detailed information, please refer to BUILDING.txt file.

# Configure SSM
---------------------------------------------------------------------------------

## Configure How to access Hadoop Namenode

   We need to let SSM know where Hadoop Namenode is. There are 2 cases,
   
### HA-Namenode
   open `smart-site.xml`, configure Hadoop cluster NameNode RPC address, fill the value field with Hadoop configuration files path, for example "file:///etc/hadoop/conf".
   
   ```xml   
   <property>
       <name>smart.hadoop.conf.path</name>
       <value>/conf</value>
       <description>local file path which holds all hadoop configuration files, such as hdfs-site.xml, core-site.xml</description>
   </property>
   ```
###  Single Namenode
   
   open `smart-site.xml`, configure Hadoop cluster NameNode RPC address,
   
 ```xml
   <property>
       <name>smart.dfs.namenode.rpcserver</name>
       <value>hdfs://namenode-ip:rpc-port</value>
       <description>Hadoop cluster Namenode RPC server address and port</description>
   </property>
  ```

###   Ignore Dirs
SSM will fetch the whole HDFS namespace by default when it starts. If you do not care about all files under some directory (for example, directory for temporary files), you can make a modification in smart-default.xml as the following shows. SSM will completely ignore the corresponding files.

Please note that SSM action will not be scheduled for file under ignored directory even though they are specified in a rule by user.

  ```xml
   <property>
       <name>smart.ignore.dirs</name>
       <value>/foodirA,/foodirB</value>
   </property>
   ```

##  **Configure Smart Server**

SSM supports running multiple Smart Servers for high-availability.  Only one of these Smart Servers can be in active state and provide services. One of the standby Smart Servers will take its place if the active Smart Server failed.

SSM also supports running one standby server with master server on a single node.

Open `servers` file under ${SMART_HOME}/conf, put each server's hostname or IP address line by line. Lines start with '#' are treated as comments.

The active SSM server is the first node in the `servers` file under ${SMART_HOME}/conf. After failover, please using the following command to find new active SSM servers
     `hadoop fs -cat /system/ssm.id `

Please note, the configuration should be the same on all server hosts.

   Optionally, JVM parameters may needs to be adjusted according to resource available (such as physical memory) to achieve better performance. JVM parameters can be configured in file ${SMART_HOME}/conf/smart-env.sh.
   Take setting maximum heap size for example, you can add the following line to the file:
   `export SSM_SERVER_JAVA_OPT="-XX:MaxHeapSize=10g"`
   It changes only Smart Servers' maximum heap size to 10GB.
   Also, the following line can be used instead:
   `export SSM_JAVA_OPT="-XX:MaxHeapSize=10g"`
   It changes heap size for all SSM services, including Smart Server and Smart Agent.

## **Configure Smart Agent (Optional)**

This step can be skipped if SSM standalone mode is preferred.
  
Open `agents` file under ${SMART_HOME}/conf, put each Smart Agent server's hostname or IP address line by line. Lines start with '#' are treated as comments. This configuration file is required by Smart Server to communicate with each Agent. So please make sure Smart Server can access these hosts by SSH without password.
After the configuration, the Smart Agents should be installed in the same path on their respective hosts as the one of Smart Server.
Smart Agent specific JVM parameters can be changed through the following way in file ${SMART_HOME}/conf/smart-env.sh.
`export SSM_AGENT_JAVA_OPT=<Your Parameters>`
 
## **Configure database**

You need to install a MySQL instance first. Then open conf/druid.xml, configure how SSM can access MySQL DB. Basically filling out the jdbc url, username and password are enough.
Please be noted that, security support will be enabled later. Here is an example for MySQL,

```xml
   <properties>
       <entry key="url">jdbc:mysql://localhost/ssm</entry>
       <entry key="username">username</entry>
       <entry key="password">password</entry>
	   ......
   </properties>	   
```
 
`ssm` is the database name. User needs to create it manually through MySQL client.

## **Configure user account to authenticate to Web UI**

By default, SSM Web UI enables user login with default user "admin", password "ssm@123".  If user wants to change the password to define more user accounts, go to the conf/shiro.ini file, 
    
`[users]` section

      define supported user name and password. It follows the username = password, role format. Here is an example,

	     admin = ssm@123, admin
	
	     ssmoperator = operator@operation, operator 
	
`[roles]` section

	 define support roles. Here is the example,

	     operator = *
	
	     admin = *

For more information about security configuration, please refer to official document

https://zeppelin.apache.org/docs/0.7.2/security/shiroauthentication.html

After finishing the SSM configuration, we can start to deploy the SSM package with the configuration files to all involved servers.


# Deploy SSM
---------------------------------------------------------------------------------

SSM supports two running modes, standalone service and SSM service with multiple Smart Agents. If file move performance is not the concern, then standalone service mode is enough. If better performance is desired, we recommend to deploy one agent on each Datanode.

To deploy SSM to Smart Server nodes and Smart Agent nodes (if configured), please execute command `./bin/install.sh` in ${SMART_HOME}. You can use --config <config-dir> to specify where SSM's config directory is. ${SMART_HOME}/conf is the default config directory if the config option is not used.
   
   ## Standalone SSM Service

For deploy standalone SSM, SSM will only start SSM server without SSM agents. Distribute `${SMART_HOME}` directory to SSM Server nodes. The configuration files are under `${SMART_HOME}/conf`.

   ## SSM Service with multiple Agents

Distribute `${SMART_HOME}` directory to SSM Server nodes and each Smart Agent nodes. Smart Agent can coexist with Hadoop HDFS Datanode. For better performance, We recommend to deploy one agent on each Datanode. Of course, Smart Agents on servers other than Datanodes and different numbers of Smart Agents than Datanodes are also supported.
On the SSM service server, switch to the SSM installation directory, ready to start and run the SSM service.


# Run SSM
---------------------------------------------------------------------------------
Enter into ${SMART_HOME} directory for running SSM. You can type `./bin/ssm version` to show specific version information for SSM.
##  **Start SSM server**
   
   SSM server requires HDFS superuser privilege to access some Namenode APIs. So please make sure the account you used to start SSM has the privilege.
   
   The start process also requires the user account to start the SSM server is able to ssh to localhost without providing password.  

   `./bin/start-ssm.sh`

   `--help` `-h` Show the usage information.

   `-format` This option is used to format the database configured for SSM use during the starting of Smart Server. All tables in this database will be dropped and then new tables will be created.

   `--config <config-dir>` can be used to specify where the config directory is.
   `${SMART_HOME}/conf` is the default config directory if the config option is not used.

   `--debug [master] [standby] [agent]` can be used to debug different targets.

   If Smart Agents are configured, the start script will start the Agents one by one remotely.
   
   Once you start the SSM server, you can open its web UI by 

   `http://Active_SSM_Server_IP:7045`

   If you meet any problem, please open the smartserver-$hostname-$user.log under ${SMART_HOME}/logs directory. All the trouble shooting clues are there.

##  **Start Smart Agent independently (Optional)**

   If you want to add more agents while keeping the SSM service online, you can run the following command on Smart Server.

   `./bin/start-agent.sh [--host .. --config ..]`

   `--debug` can be used to debug smart agent.

   `--help` `-h` Show the usage information.

   If the host option is not used, localhost is the default one. You should put the hostname specified or localhost in conf/agents.
   So all SSM services can be killed later.

   Please note that the SSM distribution directory should be under the same directory on the new agent host as that on Smart Server.

## **Stop SSM server**
   
   The script `bin/stop-ssm.sh` is used to stop SSM server.

   `./bin/stop-ssm.sh`

   `--config <config-dir>` can be used to specify where the config directory is. Please use the same config directory to execute stop-ssm.sh script as start-ssm.sh script.
   `${SMART_HOME}/conf` is the default config directory if the config option is not used.

   If Smart Agents are configured, the stop script will stop the Agents one by one remotely.


# Hadoop Configuration
----------------------------------------------------------------------------------
Please do the following configurations for integrating SSM with CDH 5.10.1, Apache Hadoop 2.7.3 or Apache Hadoop 3.1.0.

**Warning: This step may lead to `Hadoop not working issue` if it's not configured correctly. So, during testing, we don't recommend changing any configurations in Hadoop.** Actually, SSM can work with an existing Hadoop cluster without any configuration change in Hadoop. Although in that case SSM cannot collect access count or data temperature from Hadoop, you can still use SSM action to change access count or data temperature. For example, you can use `read -file XXX` to change access count or data temperature of file `XXX`.


## Apache Hadoop 2.7.3 or 3.1.0

### core-site.xml changes 

Add property `fs.hdfs.impl` to point to Smart Server provided "Smart File System". Add the following content to the `core-site.xml`
```xml
    <property>
        <name>fs.hdfs.impl</name>
        <value>org.smartdata.hadoop.filesystem.SmartFileSystem</value>
        <description>The FileSystem for hdfs URL</description>
    </property>
```
### hdfs-site.xml changes  

Add property `smart.server.rpc.address` to point to the installed Smart Server. Add the following content to the `hdfs-site.xml`. Default Smart Server RPC port is `7042`.
```xml
    <property>
        <name>smart.server.rpc.address</name>
        <value>ssm-server-ip:rpc-port</value>
    </property>
```
The value for the following property should be modified in hdfs-site.xml. It is recommended that this value should be set as follows.

value=executors*(agents+servers)*10,

in which executors represents the value for smart.cmdlet.executors in SSM and agents and servers represents the number of agents and smart servers respectively.
```xml
    <property>
        <name>dfs.datanode.balance.max.concurrent.moves</name>
        <value></value>
    </property>
```
### Storage volume types   

Make sure you have the correct HDFS storage type applied to HDFS DataNode storage volumes, here is an example which sets the RAM_DISK, SSD, DISK and Archive volumes,
```xml
     <property>
         <name>dfs.datanode.data.dir</name>
         <value>[RAM_DISK]file://${RAM_DIR}/dfs/data,[SSD]file://${hadoop.tmp.dir1}/dfs/data,[DISK]file://${hadoop.tmp.dir2}/dfs/data,[ARCHIVE]file://${hadoop.tmp.dir3}/dfs/data</value>
     </property>
```
### Check of HDFS client can access SSM jars  

Make sure Hadoop HDFS Client can access SSM jars. After we switch to the SmartFileSystem from the default HDFS implementation, we need to make sure Hadoop can access SmartFileSystem implementation jars, so that HDFS, YARN and other upper layer applications can access. There are two ways to ensure Hadoop can access SmartFileSystem,
   ####  Add SSM jars to the Hadoop classpath.

Follow the steps to add SSM Jars to classpath

  *  After SSM compilation is finished, all the SSM related jars is located in `/smart-dist/target/smart-data-{version}-SNAPSHOT/smart-data-{version}-SNAPSHOT/lib`.
  *  Distribute the jars starts with smart to user-defined SSM jars directory such as `${SSM_jars}` in each NameNode/DataNode.
  *  Add the SSM jars directory to hadoop calsspath in `hadoop-env.sh` as following.

          `export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:${SSM_jars}/*`
  
  *  For YARN and MapReduce, add the following content to the `yarn-site.xml`:

```xml
    <property>
        <name>yarn.application.classpath</name>
        <value>    
	$HADOOP_CONF_DIR:$HADOOP_COMMON_HOME/share/hadoop/common/*:$HADOOP_COMMON_HOME/share/hadoop/common/lib/*:$HADOOP_HDFS_HOME/share/hadoop/hdfs/*:$HADOOP_HDFS_HOME/share/hadoop/hdfs/lib/*:$HADOOP_YARN_HOME/share/hadoop/yarn/*:$HADOOP_YARN_HOME/share/hadoop/yarn/lib/*:${SSM_jars}/*
        </value>
    </property>
```

   #### Copy the Jars
Copy the SSM jars to the default Hadoop class path
  1. After SSM compilation is finished, all the SSM related jars is located in `/smart-dist/target/smart-data-{version}-SNAPSHOT/smart-data-{version}-SNAPSHOT/lib`.
  2. Distribute the jars starts with smart to one of default hadoop classpath in each NameNode/DataNode. For example, copy SSM jars to `$HADOOP_HOME/share/hadoop/common/lib`.


## CDH 5.10.1

### core-site.xml changes 

Add property `fs.hdfs.impl` to `core-site.xml` using Cloudera Manager to point to Smart Server provided "Smart File System".

 1.    In the Cloudera Manager Admin Console, click the HDFS indicator in the top navigation bar. Click the Configuration button.
 2.    Search `Cluster-wide Advanced Configuration Snippet (Safety Valve) for core-site.xml` configuration, add the following xml context.
```xml
    <property>
        <name>fs.hdfs.impl</name>
        <value>org.smartdata.hadoop.filesystem.SmartFileSystem</value>
        <description>The FileSystem for hdfs URL</description>
    </property>
```
 3.    Click the Save Changes button
 4.    Restart stale Services and re-deploy the client configurations


### hdfs-site.xml changes 

Add property `smart.server.rpc.address` to `hdfs-site.xml` using Cloudera Manager to point to the installed Smart Server.
 1.    In the Cloudera Manager Admin Console, click the HDFS indicator in the top navigation bar. Click the Configuration button.
 2.    Search `HDFS Service Advanced Configuration Snippet (Safety Valve) for hdfs-site.xml` configuration, add the following xml context. The  default Smart Server RPC port is `7042`.
```xml
    <property>
        <name>smart.server.rpc.address</name>
        <value>ssm-server-ip:rpc-port</value>
    </property>
```
 3.    Search `HDFS Client Advanced Configuration Snippet (Safety Valve) for hdfs-site.xml` configuration, add the following xml context. The  default Smart Server RPC port is `7042`.
```xml
    <property>
        <name>smart.server.rpc.address</name>
        <value>ssm-server-ip:rpc-port</value>
    </property>
```
 4. In HDFS configuration, dfs.datanode.balance.max.concurrent.moves should be set by

    value=executors*(agents+servers)*10,

    in which executors represents the value for smart.cmdlet.executors in SSM and agents and servers represents the number of agents and smart servers respectively.

 5.    Click the Save Changes button
 6.    Restart stale Services and re-deploy the client configurations

###  HDFS Storage types

Make sure you have the correct HDFS storage type applied to HDFS DataNode storage volumes, Check it in Cloudera Manager by the following steps.
    
 1.    In the Cloudera Manager Admin Console, click the HDFS indicator in the top navigation bar. Click the Configuration button.
 2.    Search DataNode Data Directory configuration. Below is an example which sets the RAM_DISK, SSD, DISK and Archive volumes.
```xml
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>[RAM_DISK]file://${RAM_DIR}/dfs/data,[SSD]file://${hadoop.tmp.dir1}/dfs/data,[DISK]file://${hadoop.tmp.dir2}/dfs/data,[ARCHIVE]file://${hadoop.tmp.dir3}/dfs/data</value>
    </property>
```
###  Check if HDFS can access SSM jars

After we switch to the SmartFileSystem from the default HDFS implementation, we need to make sure Hadoop can access SmartFileSystem implementation jars, so that HDFS, YARN and other upper layer applications can access. There are two ways to ensure Hadoop can access SmartFileSystem,
 
#### Add SSM jars to the CDH Hadoop Classpath using Cloudera Manager.
    
 1. In the Cloudera Manager Admin Console, click the HDFS indicator in the top navigation bar. Click the Configuration button.
 2. Search `HDFS Replication Environment Advanced Configuration Snippet (Safety Valve) for hadoop-env.sh`. Add the SSM jars to CDH Hadoop classpath. For example, `$HADOOP_CLASSPATH=$HADOOP_CLASSPATH:/PATH/TO/SSM_jars/*`
 3. Search `HDFS Service Environment Advanced Configuration Snippet (Safety Valve)`. Add the SSM jars to CDH Hadoop classpath. For example, `$HADOOP_CLASSPATH=$HADOOP_CLASSPATH:/PATH/TO/SSM_jars/*`
 4. Search `HDFS Client Environment Advanced Configuration Snippet (Safety Valve) for hadoop-env.sh`. Add the SSM jars to CDH Hadoop classpath. For example, `$HADOOP_CLASSPATH=$HADOOP_CLASSPATH:/PATH/TO/SSM_jars/*`
 5. Click the Save Changes button.
 6. In the Cloudera Manager Admin Console, click the YARN indicator in the top navigation bar. Click the Configuration button.
 7. Search `Gateway Client Environment Advanced Configuration Snippet (Safety Valve) for hadoop-env.sh`. Add the SSM jars to CDH Hadoop classpath. For example, `$HADOOP_CLASSPATH=$HADOOP_CLASSPATH:/PATH/TO/SSM_jars/*`
 8. Search `NodeManager Environment Advanced Configuration Snippet (Safety Valve)`. Add the SSM jars to CDH Hadoop classpath. For example, `$HADOOP_CLASSPATH=$HADOOP_CLASSPATH:/PATH/TO/SSM_jars/*`
 9. Search `YARN (MR2 Included) Service Environment Advanced Configuration Snippet (Safety Valve)`. Add the SSM jars to CDH Hadoop classpath. For example, `$HADOOP_CLASSPATH=$HADOOP_CLASSPATH:/PATH/TO/SSM_jars/*`
 10. Search `YARN Application Classpath`. In the classpath list, click plus symbol to open an additional row, and enter the path to SSM jars. For example, `/PATH/TO/SSM_jars/*`
 11. Search `MR Application Classpath`. In the classpath list, click plus symbol to open an additional row, and enter the path to SSM jars. For example, `/PATH/TO/SSM_jars/*`
 12. Click the Save Changes button
 13. Restart stale Services and re-deploy the client configurations.

####  Copy the SSM jars to the CDH default Hadoop class path.
     
 1. After SSM compilation is finished, all the SSM related jars is located in `/smart-dist/target/smart-data-{version}-SNAPSHOT/smart-data-{version}-SNAPSHOT/lib`.
 2. Distribute the jars starts with smart to CDH default Hadoop Classpath in each NameNode/DataNode.



## Validate the Hadoop Configuration
     After all the steps, A cluster restart is required. After the restart, try to run some simple test to see if 
the configuration takes effect. You can try TestDFSIO for example, 

 	* write data
 
	  `hadoop jar ./share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-2.6.0-cdh5.10.1-tests.jar TestDFSIO –write –nrFiles 5 –size 5MB`
   
 	* read data

	  `hadoop jar ./share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-2.6.0-cdh5.10.1-tests.jar TestDFSIO –read`

   You may want to replace the jar with the version used in your cluster. After the read data operation, if all the data files are listed on SSM web UI page "hot files" table, then the integration works very well. 


# SSM Rule Examples
---------------------------------------------------------------------------------
## **Move to SSD rule**

	`file: path matches "/test/*" and accessCount(5m) > 3 | allssd`

This rule means all the files under /test directory, if it is accessed 3 times during
last 5 minutes, SSM should trigger an action to move the file to SSD. Rule engine
will evaluate the condition every MAX{5s,5m/20} internal.


## **Move to Archive(Cold) rule**

	`file: path matches "/test/*" and age > 5h | archive`

This rule means all the files under /test directory, if it's age is more than 5 hours,
then move the file to archive storage.

## **Move one type of file to specific storage**

	`file: path matches "/test/*.xml" | allssd`

This rule will move all XML files under /test directory to SSD. In this rule, neither a
single date nor time value is specified, the rule will be evaluated every short time interval (5s by default).

## **Specify rule evaluation interval**

	`file: every 3s | path matches "/test/*.xml" | allssd`
  
This rule will move all XML files under /test directory to SSD. The rule engine will
evaluate whether the condition meets every 3s. 


## **Backup files between clusters**
     
     `file: every 500ms | path matches "/test-10000-10MB/*"| sync -dest hdfs://sr518:9000/test-10000-10MB/`
	
This rule will copy file and update any namespace changes(add,delete,rename,append) under source directory "/test-10000-10MB/" to destination directory "hdfs://sr518:9000/test-10000-10MB/". 

## **Support action chain**

	`file: path matches "/test/*" and age > 90d | archive ; setReplica 1 `
	
SSM use ";" to separate different actions in a rule. The execution trigger of later action depends on the successful execution of the prior action. If prior action fails, the following actions will not be executed.
     
Above rule means all the files under /test directory, if it's age is more than 90 days, SSM will move the file to archive storage, and set the replica to 1. "setReplica 1" is a not a built-in action. Users need to implement it by themselves.
     
Please refer to https://github.com/Intel-bigdata/SSM/blob/trunk/docs/support-new-action-guide.md for how to add a new action in SSM.
     
Rule priority and rule order will be considered to implement yet. Currently all rules
will run in parallel. For a full detail rule format definition, please refer to
https://github.com/Intel-bigdata/SSM/blob/trunk/docs/admin-user-guide.md


# Performance Tuning
---------------------------------------------------------------------------------
## Rule and Cmdlet concurrency

There are two configurable parameters which impact the SSM rule evaluation and action execution parallelism.

### smart.rule.executors

Current default value is 5, which means system will concurrently evaluate 5 rule state at the same time.
```xml
    <property>
        <name>smart.rule.executors</name>
        <value>5</value>
        <description>Max number of rules that can be executed in parallel</description>
    </property>
```
### smart.cmdlet.executors

Current default value is 10, means there will be 10 actions concurrently executed at the same time. 
If the current configuration cannot meet your performance requirements, you can change it by defining the property in the smart-site.xml under ${SMART_HOME}/conf directory. Here is an example to change the action execution parallelism to 50.
```xml
     <property>
         <name>smart.cmdlet.executors</name>
         <value>50</value>
     </property>
```
## Cmdlet history purge in metastore  

SSM choose to save cmdlet and action execution history in metastore for audit and log purpose. To not blow up the metastore space, SSM support periodically purge cmdlet and action execution history. Property `smart.cmdlet.hist.max.num.records` and `smart.cmdlet.hist.max.record.lifetime` are supported in smart-site.xml.  When either condition is met, SSM will trigger backend thread to purge the history records.
```xml
    <property>
        <name>smart.cmdlet.hist.max.num.records</name>
        <value>100000</value>
        <description>Maximum number of historic cmdlet records kept in SSM server. Oldest cmdlets will be deleted if exceeds the threshold. 
        </description>
     </property>

     <property>
        <name>smart.cmdlet.hist.max.record.lifetime</name>
        <value>30day</value>
        <description>Maximum life time of historic cmdlet records kept in SSM server. Cmdlet record will be deleted from SSM server if exceeds the threshold. Valid time unit can be 'day', 'hour', 'min', 'sec'. The minimum update granularity is 5sec.
        </description>
     </property>
```
SSM service restart is required after the configuration changes.

## Batch size of Namespace fetcher

SSM will fetch/sync namespace from namenode when it is started. According to our tests, a large namespace may lead to long start up time. To avoid this, we add a parameter named `smart.namespace.fetcher.batch`, its default value is 500. You can change it if namespace is very large, e.g., 100M or more. A larger batch size will greatly speed up fetcher efficiency, and reduce start up time.
```xml
    <property>
        <name>smart.namespace.fetcher.batch</name>
        <value>500</value>
        <description>Batch size of Namespace fetcher</description>
    </property>
```
SSM supports concurrently fetching namespace. You can set a large value for each of the following two properties to increase concurrency.
```xml
    <property>
        <name>smart.namespace.fetcher.producers.num</name>
        <value>3</value>
        <description>Number of producers in namespace fetcher</description>
    </property>

    <property>
        <name>smart.namespace.fetcher.consumers.num</name>
        <value>6</value>
        <description>Number of consumers in namespace fetcher</description>
    </property>
```
##  Disable SSM SmartDFSClient

For some reasons, if you do want to disable SmartDFSClients on a specific host from contacting SSM server, it can be realized by using the following commands. After that, newly created SmartDFSClients on that node will not try to connect SSM server while other functions (like HDFS read/write) will remain unaffected.

To disable SmartDFSClients on hosts:
`./bin/disable-smartclient.sh --hosts <host names or ips>`
For example: ./bin/disable-smartclient.sh --hosts hostA hostB hostC 192.168.1.1
Or you can write all the host names or ips into a file, one name or ip each line. Then you can using the following command to do the same thing:
`./bin/disable-smartclient.sh --hostsfile <file path>`

After that if you want to re-enable, then the following commands can be used:
`./bin/enable-smartclient.sh --hosts <host names or ips>`
or
`./bin/enable-smartclient.sh --hostsfile <file path>`
The arguments are same with `disable-smartclient.sh`
Note: To make the scripts work, you have to set up password-less SSH connections between the node that executing these scripts and the rest hosts.


# Trouble Shooting
---------------------------------------------------------------------------------
 Logs for master server will go to smartserver-master-$hostname-$user.log under ${SMART_HOME}/logs directory.

 Logs for standby server will go to smartserver-standby-$hostname-$user.log under ${SMART_HOME}/logs directory.

 Logs for agent will go to smartagent-$hostname-$user.log under ${SMART_HOME}/logs directory.

1. Smart Server can't start successfully

   a. Check whether Hadoop HDFS NameNode is running
   
   b. Check whether MySQL server is running
   
   c. Check if there is already a SmartDaemon process running
   
   d. Got to logs under ${SMART_HOME}/logs directory, find any useful clues in the log file.
   
2. UI can not show hot files list 

   Possible causes:
  
   a. Cannot lock system mover locker. You may see something like follows in the smartserver-$hostname-$user.log file. Make sure there is no system mover running. Try to restart the SSM service will solve the problem.

```
   2017-07-15 00:38:28,619 INFO org.smartdata.hdfs.HdfsStatesUpdateService.init 68: Initializing ...
   2017-07-15 00:38:29,350 ERROR org.smartdata.hdfs.HdfsStatesUpdateService.checkAndMarkRunning 138: Unable to lock 'mover', please stop 'mover' first.
   2017-07-15 00:38:29,350 INFO org.smartdata.server.engine.StatesManager.initStatesUpdaterService 180: Failed to create states updater service.
```

3. MySQL related "Specified key was too long; max key length is 767 bytes"

    This problem is caused by MySQL version below requirement (MySQL 5.7 or higher is required). Because index length of MySQL version <= 5.6 cannot exceeds 767 bytes. We have submitted several patches for this issue. But, the best solution is upgrading your MySQL to a higher version, e.g., 5.7. For more details, please read these articles [Limits on InnoDB Tables](https://dev.mysql.com/doc/refman/5.5/en/innodb-restrictions.html) and [Maximum Column Size is 767 bytes Constraint in MySQL](https://community.pivotal.io/s/article/Apps-are-down-due-to-the-Maximum-Column-Size-is-767-bytes-Constraint-in-MySQL). 

	 
Notes
---------------------------------------------------------------------------------
1. If there is no SSD or Archive type disk volume configured in DataNodes, "allssd" or "archive" action will fail.
2. When SSM starts, it will pull the whole namespace from Namenode. If the namespace is very huge, it will take long time for SSM to finish namespace synchronization. SSM may half function during this period.


   
