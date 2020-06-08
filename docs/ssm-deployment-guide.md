# SSM Deployment Guide with Hadoop
----------------------------------------------------------------------------------
## Requirements:

* Unix/Unix-like OS
* JDK 1.7 for CDH 5.10.1 or JDK 1.8 for Apache Hadoop 2.7.3/3.1.0
* CDH 5.10.1 or Apache Hadoop 2.7.3/3.1.0
* MySQL Community 5.7.18+
* Maven 3.1.1+ (merely for build use)

## Why JDK 1.7 is preferred for CDH 5.10.1

  Because by default CDH 5.10.1 supports compiling and running with JDK 1.7. If you
  want to use JDK1.8, please refer to Cloudera web site for how to support JDK1.8
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

A tar distribution package will be generated under 'smart-dist/target'. Unzip the tar distribution package to ${SMART_HOME} directory, and SSM configuration files are under '${SMART_HOME}/conf'.
For more detailed information, please refer to BUILDING.txt file.

# Configure SSM
---------------------------------------------------------------------------------

## Configure How to access Hadoop Namenode

   We need to let SSM know where Hadoop Namenode is. There are 2 options to user.
   
### HA-Namenode
   In `smart-site.xml`, configure `smart.hadoop.conf.path` with specifying the path of Hadoop configuration directory.
   
   ```xml   
   <property>
       <name>smart.hadoop.conf.path</name>
       <value>/conf</value>
       <description>Hadoop configuration path. The typical path is $HADOOP_HOME/etc/hadoop</description>
   </property>
   ```
###  Single Namenode
   
   In `smart-site.xml`, configure Hadoop cluster active NameNode RPC address.
   
 ```xml
   <property>
       <name>smart.dfs.namenode.rpcserver</name>
       <value>hdfs://namenode-ip:rpc-port</value>
       <description>Hadoop cluster active Namenode RPC server address and port</description>
   </property>
  ```

###   Ignore Dirs (Optional)
SSM will fetch the whole HDFS namespace by default when it starts. If you do not care about files under some directory (for example, directory for temporary files), you can make the configuration in smart-default.xml to ignore these directories as the following shows. SSM will completely ignore the corresponding files.

Please note that SSM action will not be scheduled for files under ignored directory even though they are specified in a rule by user.

  ```xml
   <property>
       <name>smart.ignore.dirs</name>
       <value>/foodirA,/foodirB</value>
   </property>
   ```

###   Cover Dirs (Optional)
SSM will fetch the whole HDFS namespace by default when it starts. If you only care about the files under some directory, you can make a modification in smart-default.xml as the following shows.
SSM will only fetch files under the given directories. For more than one directories, they should be separated by ",".

The access info and other info related to fetched files will be considered. For other files not under the fetched directories, their info will be ignored.

  ```xml
   <property>
       <name>smart.cover.dirs</name>
       <value>/foodirA,/foodirB</value>
   </property>
   ```

###   Work Dir (Optional)
This HDFS directory is used as a tmp directory for SSM to store tmp files and data. The default path is "/system/ssm", and SSM will ignore files under the tmp directory.
Only one directory can be set for this property.

  ```xml
   <property>
       <name>smart.work.dir</name>
       <value>/system/ssm</value>
   </property>
   ```

##  **Configure Smart Server**

SSM supports running multiple Smart Servers for high-availability.  Only one of these Smart Servers can be in active state and provides services. One of the standby Smart Servers will take its place if the active Smart Server fails.

SSM also supports running one standby server with master server on a single node.

Open `servers` file under ${SMART_HOME}/conf, put each server's hostname or IP address line by line. Content starts with '#' is treated as comment.

The active Smart Server is the first node in the `servers` file under ${SMART_HOME}/conf. After failover, please use the following command to find new active Smart Server.
     `hadoop fs -cat /system/ssm.id `

Please note, the configuration should be same on all server hosts.

   Optionally, JVM parameters may need to be adjusted according to available resource (such as physical memory) to achieve better performance. JVM parameters can be configured in file ${SMART_HOME}/conf/smart-env.sh.
   Take maximum heap size configuration for example. By adding the following line to the file:
   `export SSM_SERVER_JAVA_OPT="-XX:MaxHeapSize=10g"`,
   the maximum heap size for each Smart Server can be changed to 10GB.
   Also, the following line can be used instead:
   `export SSM_JAVA_OPT="-XX:MaxHeapSize=10g"`,
   which changes the maximum heap size for all SSM services, including Smart Server and Smart Agent.

## **Configure Smart Agent (Optional)**

This step can be skipped if SSM standalone mode is preferred.
  
Open `agents` file under ${SMART_HOME}/conf, and put each Smart Agent server's hostname or IP address line by line. Content starts with '#' is treated as comment.
The start script will access each Smart Agent host via SSH to launch the service.
So please make sure that login by SSH without password is configured.
After the configuration, the Smart Agents should be installed in the same path on their respective hosts as that of Smart Server.
The specific JVM parameters for Smart Agent can be changed through the following way in file ${SMART_HOME}/conf/smart-env.sh.
`export SSM_AGENT_JAVA_OPT=<Your Parameters>`
 
## **Configure database**

You need to install a MySQL instance first. The jdbc url, username and password should be configured in conf/druid.xml for SSM connecting to the database.
Please note that security support will be enabled later. Here is an example for MySQL,

```xml
   <properties>
       <entry key="url">jdbc:mysql://localhost/ssm</entry>
       <entry key="username">username</entry>
       <entry key="password">password</entry>
	   ......
   </properties>	   
```
 
`ssm` is the database name. User needs to create it manually through MySQL client.

Alternatively, user can configure database password by hadoop credential provider instead making the password configured visibly in druid.xml.
The below command shows how to set SSM metastore password into a jceks file. Please note the alias name is `smart.metastore.password`.

`hadoop credential create smart.metastore.password -value 123456 -provider jceks://file/root/ssm.jceks`

Then, the above jceks path should be specified in smart-default.xml.

```xml
  <property>
    <name>hadoop.security.credential.provider.path</name>
    <value>jceks://file/root/ssm.jceks</value>
  </property>
```

If this property is not set or exception occurs when SSM tries to get metastore password from jceks, SSM will use the one configured in druid.xml.

For getting more details, please refer to https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/CredentialProviderAPI.html.

## **Configure SSM user account**

By default, anonymous user can login on SSM web UI without authentication. To address security concerns, user can refer to [web-authentication-enable-guide.md](https://github.com/Intel-bigdata/SSM/blob/trunk/docs/web-authentication-enable-guide.md)
to enable the authentication. After that, please login with the default username and password (admin/ssm@123).

Authenticated user can change password on SSM web UI (top right corner, admin/Change Password) after login.

Admin can also add new user on web UI (admin/Add New User). Please note only admin user has this permission.

# Deploy SSM
---------------------------------------------------------------------------------

SSM supports two running modes, standalone service mode and distributed service mode. If performance is not the concern, standalone service mode is enough. If better performance is desired, we recommend deploying one agent on each Datanode.

To deploy SSM to Smart Server nodes and Smart Agent nodes (if configured), please execute command `./bin/install.sh` in ${SMART_HOME}. You can use --config <config-dir> to specify where SSM's config directory is. ${SMART_HOME}/conf is the default config directory if the config option is not used.
   
## Standalone service mode

For deploying SSM in standalone service mode, because only Smart Server needs to be launched, you can just distribute `${SMART_HOME}` directory to Smart Server node. The default conf directory `${SMART_HOME}/conf`.

## Distributed service mode

Distribute `${SMART_HOME}` directory to each Smart Server node and each Smart Agent node. Smart Agent can coexist with HDFS Datanode or not on a host, so the following kinds of deployment are also workable.

1) Smart Agent is deployed on a server that does not serve as Datanode.
2) The number of Smart Agents is different from that of Datanodes.

On a SSM server, user can switch to the SSM installation directory to run all SSM services. Please sync the time for each node to make sure consistent time stamp is used for action and database table.


## SSM HA

In SSM distributed service mode, SSM HA can be enabled by specifying at least two Smart Servers in `conf/servers`. One will serve as master server and the other(s) will serve as standby server. Standby server can also execute SSM action, like a Smart Agent.
When master server halts, one standby server will be elected as master and then shift its role from standby to master. You can start a standby service on previous halted master server to resume the original HA deployment.
Please see [link](#start-standby-smart-server-for-active-ssm-cluster-optional).

# Run SSM
---------------------------------------------------------------------------------
Enter ${SMART_HOME} directory for running SSM. You can type `./bin/ssm version` to show specific version information of SSM.
##  **Start SSM server**
   
   SSM server requires HDFS superuser privilege to access some Namenode APIs. So please make sure the account you used to start SSM has the privilege.
   
   It is required that the OS user is able to ssh to localhost without password for starting Smart Server.

   `./bin/start-ssm.sh`

   `--help` `-h` Show the usage information.

   `-format` This option is used to format the database configured for SSM use during the starting of Smart Server. All tables in this database will be dropped and then new tables will be created.

   `--config <config-dir>` can be used to specify where the config directory is.
   `${SMART_HOME}/conf` is the default config directory if the config option is not used.

   `--debug [master] [standby] [agent]` can be used to debug different targets.

   If Smart Agents are configured, the start script will start the Agents one by one remotely.
   
   Once you start the SSM server, you can open its web UI by 

   `http://Active_SSM_Server_IP:7045`

   If you meet any problem, please try to find trouble shooting clues in log file smartserver-$hostname-$user.log under ${SMART_HOME}/logs directory. All the trouble shooting clues are there.

##  **Start Smart Agent for active SSM cluster (Optional)**

   If you want to scale out SSM cluster while keeping SSM service active, you can run the following command on any node to start more Agents.

   `./bin/start-agent.sh [--host $hostname --config $config_dir --debug]`

   `--host` allows user to specify the hostname of Smart Agent. If it is not specified, localhost will be used. You can also specify multiple nodes, e.g., --host "node1 node2".

   `--config` allows user to specify a config dir for Agent. There is no need to use this option if the default ${SSM_HOME}/conf is used.

   `--debug` can be used to debug Smart Agent, it is just useful for developers.

   `--help` or `-h` shows the usage information.

   You should put the hostname specified or localhost in conf/agents. Thus, the newly added Agent can be killed by `stop-ssm.sh`.

   Please note that SSM dist package should be put under the same directory on the new Agent host as that on Smart Server.

##  **Start Standby Smart Server for active SSM cluster (Optional)**

   If you want to launch standby Smart Server while keeping SSM service active, you can run the following command on standby server or any other server.

   `./bin/start-standby-server.sh [--host $hostname --config $config_dir --debug]`

   `--host` allows user to specify the hostname of standby Smart Server. If it is not specified, localhost will be used. You can also specify multiple nodes, e.g., --host "node1 node2".

   `--config` allows user to specify a config dir. There is no need to use this option if the default ${SSM_HOME}/conf is used.

   `--debug` can be used to debug standby Smart Server, it is just useful for developers.

   `--help` or `-h` shows the usage information.

   The general purpose of this script is to start a failed Smart Server that configured in conf/servers beforehand, which may meet most user cases.

## **Stop SSM server**
   
   The script `bin/stop-ssm.sh` is used to stop SSM server.

   `./bin/stop-ssm.sh`

   `--config <config-dir>` can be used to specify where the config directory is. Please use the same config directory to execute stop-ssm.sh script as start-ssm.sh script.
   `${SMART_HOME}/conf` is the default config directory if the config option is not used.

   If Smart Agents are configured, the stop script will stop the Agents one by one remotely.


# Hadoop Configuration
----------------------------------------------------------------------------------
Please follow the below configuration guide to integrate SSM with CDH 5.10.1, Apache Hadoop 2.7.3 or Apache Hadoop 3.1.0.

**Warning: This step may lead to `Hadoop not working issue` if it's not configured correctly. So, during testing, we do not recommend changing any configurations in Hadoop.** Actually, SSM can work with an existing Hadoop cluster without any configuration change in Hadoop. Although in that case SSM cannot collect access count or data temperature from Hadoop, you can still use SSM action to change access count or data temperature. For example, you can use `read -file XXX` to change access count or data temperature of file `XXX`.


## Apache Hadoop 2.7.3 or 3.1.0

### core-site.xml changes 

Add property `fs.hdfs.impl` to point to Smart Server which provides "Smart File System". Add the following content to the `core-site.xml`
```xml
    <property>
        <name>fs.hdfs.impl</name>
        <value>org.smartdata.hadoop.filesystem.SmartFileSystem</value>
        <description>The FileSystem for hdfs URL</description>
    </property>
```
### hdfs-site.xml changes  

Add property `smart.server.rpc.address` to point to the installed Smart Server. Add the following content to the `hdfs-site.xml`. Default Smart Server RPC port is `7042`.
If SSM HA mode is enabled, more than one Smart Server address can be specified with comma delimited.
```xml
    <property>
        <name>smart.server.rpc.address</name>
        <value>smart-server-hostname:rpc-port</value>
    </property>
```

The cover dirs or ignore dirs should also be configured in hdfs-site.xml if user wants SSM to only monitor some HDFS dirs or just ignore some other HDFS dirs.
Please refer to the above Cover Dirs & Ignore Dirs section.

The value for the following property should be modified in hdfs-site.xml. This value is recommended to be set as follows.

value=executors_num*(agents_num+servers_num)*10,

in which executors_num, agents_num and servers_num represent the value of smart.cmdlet.executors, the number of smart agents and the number of smart servers respectively.
```xml
    <property>
        <name>dfs.datanode.balance.max.concurrent.moves</name>
        <value></value>
    </property>
```
### Storage volume types   

Make sure you have the correct HDFS storage type applied to HDFS DataNode storage volumes, and the below is an example which sets the RAM_DISK, SSD, DISK and Archive volumes.
```xml
     <property>
         <name>dfs.datanode.data.dir</name>
         <value>[RAM_DISK]file://${RAM_DIR}/dfs/data,[SSD]file://${hadoop.tmp.dir1}/dfs/data,[DISK]file://${hadoop.tmp.dir2}/dfs/data,[ARCHIVE]file://${hadoop.tmp.dir3}/dfs/data</value>
     </property>
```
### Check if HDFS client can access SSM jars

Make sure Hadoop HDFS Client can access SSM jars. After we switch to the SmartFileSystem from the default HDFS implementation, we need to make sure Hadoop can access SmartFileSystem implementation jars. So SmartFileSystem can be used by HDFS, YARN and other upper layer applications. There are two ways to ensure Hadoop can access SmartFileSystem.
   ####  Add SSM jars to the Hadoop classpath.

Follow the steps to add SSM Jars to classpath

  *  After SSM compilation is finished, all the SSM related jars is located in `/smart-dist/target/smart-data-{version}-SNAPSHOT/smart-data-{version}-SNAPSHOT/lib`.
  *  Distribute the jars whose names are prefixed with smart into user-defined directory such as `${SSM_jars}` in each NameNode/DataNode.
  *  Add the SSM jars directory to hadoop classpath in `hadoop-env.sh` as the following shows.

          export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:${SSM_jars}/*

     The libs under SSM home can also be used in the above setting.

          export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:${SSM_HOME}/lib/smart*

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
Copy the SSM jars to the default Hadoop classpath.
  1. After SSM compilation is finished, all the SSM related jars are located in `/smart-dist/target/smart-data-{version}-SNAPSHOT/smart-data-{version}-SNAPSHOT/lib`.
  2. Distribute the jars whose names are prefixed with "smart" to hadoop classpath in each NameNode/DataNode. For example, copy SSM jars to `$HADOOP_HOME/share/hadoop/common/lib`.


## CDH 5.10.1

### core-site.xml changes 

Add property `fs.hdfs.impl` to `core-site.xml` using Cloudera Manager to point to Smart Server which provides "Smart File System".

 1.    In the Cloudera Manager Admin Console, click the HDFS indicator in the top navigation bar. Click the Configuration button.
 2.    Search `Cluster-wide Advanced Configuration Snippet (Safety Valve) for core-site.xml` configuration, and add the following xml context.
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
 2.    Search `HDFS Service Advanced Configuration Snippet (Safety Valve) for hdfs-site.xml` configuration, and add the following xml context. The  default Smart Server RPC port is `7042`.
       If SSM HA mode is enabled, more than one Smart Server address can be specified with comma delimited.
```xml
    <property>
        <name>smart.server.rpc.address</name>
        <value>smart-server-hostname:rpc-port</value>
    </property>
```
 3.    Search `HDFS Client Advanced Configuration Snippet (Safety Valve) for hdfs-site.xml` configuration, and add the following xml context. The  default Smart Server RPC port is `7042`.
```xml
    <property>
        <name>smart.server.rpc.address</name>
        <value>ssm-server-ip:rpc-port</value>
    </property>
```
 4. In HDFS configuration, dfs.datanode.balance.max.concurrent.moves should be set by

    value=executors_num*(agents_num+servers_num)*10,

    in which executors_num, agents_num and servers_num represent the value of smart.cmdlet.executors, the number of smart agents and the number of smart servers respectively.

 5. The cover dirs or ignore dirs should also be configured in hdfs-site.xml if user wants SSM to only monitor some HDFS dirs or just ignore some other HDFS dirs.
    Please refer to the above Cover Dirs & Ignore Dirs section.

 6.    Click the Save Changes button
 7.    Restart stale Services and re-deploy the client configurations

###  HDFS Storage types

Make sure you have the correct HDFS storage type applied to HDFS DataNode storage volumes. Check it in Cloudera Manager through the following steps.
    
 1.    In the Cloudera Manager Admin Console, click the HDFS indicator in the top navigation bar. Click the Configuration button.
 2.    Search DataNode Data Directory configuration. Below is an example which sets the RAM_DISK, SSD, DISK and Archive volumes.
```xml
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>[RAM_DISK]file://${RAM_DIR}/dfs/data,[SSD]file://${hadoop.tmp.dir1}/dfs/data,[DISK]file://${hadoop.tmp.dir2}/dfs/data,[ARCHIVE]file://${hadoop.tmp.dir3}/dfs/data</value>
    </property>
```
###  Check if HDFS can access SSM jars

After we switch to the SmartFileSystem from the default HDFS implementation, we need to make sure Hadoop can access SmartFileSystem implementation jars. So SmartFileSystem can be used by HDFS, YARN and other upper layer applications. There are two ways to ensure Hadoop can access SmartFileSystem.
 
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
     
 1. After SSM compilation is finished, all the SSM related jars are located in `/smart-dist/target/smart-data-{version}-SNAPSHOT/smart-data-{version}-SNAPSHOT/lib`.
 2. Distribute the jars whose names are prefixed with smart to CDH default Hadoop Classpath in each NameNode/DataNode.



## Validate the Hadoop Configuration

After all these steps, a cluster restart is required. After the restart, try to run some simple test to see if the configuration takes effect. For example, you can try to run TestDFSIO workload.

 	* write data
 
	  `hadoop jar ./share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-2.6.0-cdh5.10.1-tests.jar TestDFSIO –write –nrFiles 5 –size 5MB`
   
 	* read data

	  `hadoop jar ./share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-2.6.0-cdh5.10.1-tests.jar TestDFSIO –read`

   You may want to replace the jar with the version used in your cluster. After the read operation, if all the data files are listed in "hot files" table on SSM web UI page, it proves that the integration works very well.


# SSM Rule Examples
---------------------------------------------------------------------------------
## **Move to SSD rule**

	`file: path matches "/test/*" and accessCount(5m) > 3 | allssd`

This rule means for each file under /test directory, if it is accessed 3 times during
last 5 minutes, SSM should trigger an action to move the file to SSD. Rule engine
will evaluate the condition every MAX{5s,5m/20} interval.


## **Move to Archive (Cold) rule**

	`file: path matches "/test/*" and age > 5h | archive`

This rule means for each file under /test directory, if its age is more than 5 hours,
then move the file to archive storage.

## **Move one type of file to specific storage**

	`file: path matches "/test/*.xml" | allssd`

This rule will move all XML files under /test directory to SSD. In this rule, neither a
single date nor time interval value is specified, and the rule will be evaluated every short time interval (5s by default).

## **Specify rule evaluation interval**

	`file: every 3s | path matches "/test/*.xml" | allssd`
  
This rule will move all XML files under /test directory to SSD. The rule engine will
evaluate whether the condition is met every 3s.


## **Backup files between clusters**
     
     `file: every 500ms | path matches "/test-10000-10MB/*"| sync -dest hdfs://sr518:9000/test-10000-10MB/`
	
This rule will copy files and update all namespace changes(add,delete,rename,append) under source directory "/test-10000-10MB/" to destination directory "hdfs://sr518:9000/test-10000-10MB/".

## **Support action chain**

	`file: path matches "/test/*" and age > 90d | archive ; setReplica 1 `
	
In SSM rule, ";" is used to separate two or more actions. These actions are executed in sequence.
If a prior action fails, the followed actions will not be executed.
     
The above rule means for each file under /test directory, if its age is more than 90 days, SSM will move the file to archive storage, and set the replica to 1. "setReplica 1" is not a built-in action. Users need to implement it by themselves.
     
Please refer to https://github.com/Intel-bigdata/SSM/blob/trunk/docs/support-new-action-guide.md for how to add a new action in SSM.
     
Rule priority and rule order will be considered to implement in the future. Currently all rules
will run in parallel. For a full detailed rule format definition, please refer to
https://github.com/Intel-bigdata/SSM/blob/trunk/docs/admin-user-guide.md


# Performance Tuning
---------------------------------------------------------------------------------
## Rule and Cmdlet concurrency

There are two configurable parameters which impact the SSM rule evaluation and action execution parallelism.

### smart.rule.executors

The default value is 5, which means system will concurrently evaluate 5 rule states at the same time.
```xml
    <property>
        <name>smart.rule.executors</name>
        <value>5</value>
        <description>Max number of rules that can be executed in parallel</description>
    </property>
```
### smart.cmdlet.executors

The default value is 10, which means there will be 10 actions executed concurrently at the same time.
If the current configuration cannot meet your performance requirements, you can change it by defining the property in the smart-site.xml under ${SMART_HOME}/conf directory. Here is an example showing how to change the action execution parallelism to 50.
```xml
     <property>
         <name>smart.cmdlet.executors</name>
         <value>50</value>
     </property>
```
## Cmdlet history purge in metastore  

SSM will save cmdlet and action execution history in metastore for audit and log purposes. To avoid blowing up the metastore space, SSM supports configuration for periodically purging historical records for cmdlets and actions.
Property `smart.cmdlet.hist.max.num.records` and `smart.cmdlet.hist.max.record.lifetime` are supported in smart-site.xml.  When either condition is met, SSM will trigger backend thread to purge the history records.
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

SSM will fetch/sync namespace from namenode when it is started. According to our tests, a large namespace may lead to long start up time. To avoid this, we add a parameter named `smart.namespace.fetcher.batch`, whose default value is 500. You can change it if namespace is very large, e.g., 100M or more. A larger batch size will greatly speed up fetcher efficiency, and reduce start up time.
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

If you do want to disable SmartDFSClients on a specific host from connecting to SSM server for reporting access event, it can be realized by using the following commands.

To disable SmartClient on a host:

`./bin/disable-smartclient.sh --hosts <host names or ips>`

For example: ./bin/disable-smartclient.sh --hosts hostA hostB hostC 192.168.1.1

Or you can write all the host names or ips into a file, one name or ip each line. Then you can use the following command to do the same thing:

`./bin/disable-smartclient.sh --hostsfile <file path>`

Then, newly created SmartDFSClients on that node will not try to connect SSM server while other functions (like HDFS read/write) will remain unaffected.

In essence, /tmp/SMART_CLIENT_DISABLED_ID_FILE is created to tell SmartDFSClient that it should not report access event to Smart Server.

If you want to re-enable SmartDFSClients connect to Smart Server, the following commands can be used:

`./bin/enable-smartclient.sh --hosts <host names or ips>`

or

`./bin/enable-smartclient.sh --hostsfile <file path>`

The arguments are the same as `disable-smartclient.sh`

Note: To make the scripts work, you have to set up SSH password-less connections between the node that executes these scripts and the rest hosts.

# Trouble Shooting
---------------------------------------------------------------------------------
 Logs for master server will go to smartserver-master-$hostname-$user.log under ${SMART_HOME}/logs directory.

 Logs for standby server will go to smartserver-standby-$hostname-$user.log under ${SMART_HOME}/logs directory.

 Logs for agent will go to smartagent-$hostname-$user.log under ${SMART_HOME}/logs directory.

1. Smart Server can't start successfully

   a. Check whether Hadoop HDFS NameNode is running
   
   b. Check whether MySQL server is running
   
   c. Check if there is already a SmartDaemon process running

   d. Check logs under ${SMART_HOME}/logs directory, to find any useful clues in the log file.
   
2. UI can not show hot files list 

   Possible causes:
  
   a. Cannot lock system mover locker. You may see something like follows in the smartserver-$hostname-$user.log file. Make sure there is no system mover running. Try to restart the SSM service to solve the problem.

```
   2017-07-15 00:38:28,619 INFO org.smartdata.hdfs.HdfsStatesUpdateService.init 68: Initializing ...
   2017-07-15 00:38:29,350 ERROR org.smartdata.hdfs.HdfsStatesUpdateService.checkAndMarkRunning 138: Unable to lock 'mover', please stop 'mover' first.
   2017-07-15 00:38:29,350 INFO org.smartdata.server.engine.StatesManager.initStatesUpdaterService 180: Failed to create states updater service.
```

3. MySQL related "Specified key was too long; max key length is 767 bytes"

    This problem will occur when MySQL version does not meet the requirement of SSM (MySQL 5.7 or higher is required). Because index length of MySQL version <= 5.6 cannot exceed 767 bytes.
    We have submitted several patches for this issue.Also note that some character take up more than one byte, such as chinese with utf-8 take up 3 bytes.
    You can set how many bytes that one character takes up, default value is 1. But the best solution is upgrading your MySQL to a higher version, e.g., 5.7.
    For more details, please read these articles:

    [Limits on InnoDB Tables](https://dev.mysql.com/doc/refman/5.5/en/innodb-restrictions.html)

    [Maximum Column Size is 767 bytes Constraint in MySQL](https://community.pivotal.io/s/article/Apps-are-down-due-to-the-Maximum-Column-Size-is-767-bytes-Constraint-in-MySQL).

```xml
   <property>
     <name>smart.metastore.character.takeup.bytes</name>
     <value>1</value>
     <description>
       How many bytes that one character takes up, default value is 1.
       Used to compatible with mysql5.6.
     </description>
   </property>
```

Notes
---------------------------------------------------------------------------------
1. If there is no SSD or Archive type disk volume configured in DataNodes, "allssd" or "archive" action will fail.
2. When SSM starts, it will pull the whole namespace from Namenode. If the namespace is very huge, it will take long time for SSM to finish namespace synchronization. SSM cannot provide complete functions during this period.


   
