Deployment SSM with Hadoop(CDH5.10.1 or Hadoop 2.7) Guide
----------------------------------------------------------------------------------
Requirements:

* Unix System
* JDK 1.7 for CDH5.10.1
* JDK 1.8 for Hadoop 2.7
* CDH 5.10.1
* Hadoop 2.7
* MySQL Community 5.7.18
* Maven 3.1.1+


Why JDK 1.7 is preferred
----------------------------------------------------------------------------------

  It is because by default CDH5.10.1 supports compile and run with JDK 1.7. If you
  want to use JDK1.8, please turn to Cloudera web site for how to support JDK1.8
  in CDH5.10.1.
  For SSM, JDK 1.7 and 1.8 are both supported.


Build SSM Package
---------------------------------------------------------------------------------
* **Download SSM**

  Download SSM branch from Github https://github.com/Intel-bigdata/SSM/ 

* **Build SSM**

  * For CDH5.10.1
  
  	`mvn package -Pdist,web,hadoop-cdh-2.6 -DskipTests`
   
  * For Hadoop 2.7
  	
	`mvn package -Pdist,web,hadoop-2.7 -DskipTests`


   A tar distribution package will be generated under 'smart-dist/target'. unzip the tar distribution package to get the configuration files under './conf'. 
   More detail information, please refer to BUILDING.txt file.

Configure SSM
---------------------------------------------------------------------------------

* **Configure How to access Hadoop Namenode**

   We need to let SSM know where Hadoop Namenode is. There are 2 cases,
   
   a.  HA Namenode
   
   open `smart-site.xml`, configure Hadoop cluster NameNode RPC address, fill the value field with Hadoop configuration files path, for example "file:///etc/hadoop/conf".
   
   ```xml   
   <property>
       <name>smart.hadoop.conf.path</name>
       <value>/conf</value>
       <description>local file path which holds all hadoop configuration files, such as hdfs-site.xml, core-site.xml</description>
    </property>
   ``` 
   
   b.  Single Namenode
   
   open `smart-site.xml`, configure Hadoop cluster NameNode RPC address,
   
   ```xml
   <property>
       <name>smart.dfs.namenode.rpcserver</name>
       <value>hdfs://namenode-ip:port</value>
       <description>Hadoop cluster Namenode RPC server address and port</description>
   </property>
   ```

   SSM will fetch the whole HDFS namespace when it starts by default. If you do not care about files under some directories (directories for temporary files for example) then you can configure them in the following way, SSM will completely ignore these files. Please note, actions will also not be triggered for these files by rules.

   ```xml
   <property>
       <name>smart.ignore.dirs</name>
       <value>/foodirA,/foodirB</value>
   </property>
   ```
* **Configure Smart Server**

   SSM supports running multiple Smart Servers for high-availability. Only one of these Smart Servers can be in active state and provide services. One of the standby Smart Servers will take its place if the active Smart Server failed.

   Open `servers` file under /conf, put each server's hostname or IP address line by line. Lines start with '#' are treated as comments.

   Please note, the configuration should be the same on all server hosts.

* **Configure Smart Agent (optional)**

   This step can be skipped if SSM standalone mode is preferred.
  
   Open `agents` file under /conf, put each Smart Agent server's hostname or IP address line by line. Lines start with '#' are treated as comments. This configuration file is required by Smart Server to communicate with each Agent. So please make sure Smart Server can access these hosts by SSH without password.
   After the configuration, the Smart Agents should be installed in the same path on their respective hosts as the one of Smart Server.
 
* **Configure database**

   SSM currently supports MySQL and TiDB (release-1.0.0 version) as the backend to store metadata. TiDB is a distributed NewSQL database, which can provide good scalability and high availability for SSM.

   You just need to follow the guide in one of the two following options to configure database for SSM.

   * Option 1. Use MySQL

    You need to install a MySQL instance first. Then open conf/druid.xml, configure how SSM can access MySQL DB. Basically filling out the jdbc url, username and password are enough.
    Please be noted that, security support will be enabled later. Here is an example for MySQL,
   
   ```xml
   <properties>
       <entry key="url">jdbc:mysql://localhost/ssm</entry>
       <entry key="username">root</entry>
       <entry key="password">123456</entry>
	   ......
   </properties>	   
   ```
   
   `ssm` is the database name. User needs to create it manually through MySQL client.

   * Option 2. Use SSM-TiDB

    To use TiDB, three shared libraries should be built beforehand and put into ${SMART_HOME}/lib. For build guide, you can refer to https://github.com/Intel-bigdata/ssm-tidb/tree/release-1.0.0.

    TiDB can be enabled in smart-site.xml.

   ```xml
    <property>
        <name>smart.tidb.enable</name>
        <value>true</value>
        ......
    </property>
   ```

    For SSM standalone mode, the three instances PD, TiKV and TiDB are all deployed on Smart Server host.
    For SSM with multiple agents mode, Smart Server will run PD and TiDB instance and each agent will run a TiKV instance.
    So the storage capacity of SSM-TiDB can easily be scaled up by just adding more agent server. This is a great advantage over using MySQL.

    If TiDB is enabled, there is no need to configure jdbc url in druid.xml. In TiDB only root user is created initially, so you should set username as root. Optionally, you can set a password for root user in druid.xml.

    An example of configuration in druid.xml for using TiDB is shown as follows.

   ```xml
    <properties>
        <!-- <entry key="url">jdbc:mysql://127.0.0.1:4000/test</entry> no need to configure url for TiDB -->
        <entry key="username">root</entry>
        <entry key="password"></entry>
        ......
    <properties>
   ```

    TiDB supports the usage of MySQL shell. The way of MySQL shell connecting to TiDB server is as same as that for MySQL.
    If user password is not set in druid, by default the command to enter into MySQL shell on Smart Server is `mysql -h 127.0.0.1 -u root -P 7070`.
    The 7070 port is the default one configured for tidb.service.port in smart-default.xml.
    If you modify it, the port in the above command should also be modified accordingly.
    In TiDB, the database named ssm is used to store metadata.

    By default, the logs of Pd, TiKV and TiDB are under ${SMART_HOME}/logs directory. You can refer to these logs if encountering database fault.

* **Configure user account to authenticate to Web UI**

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


Deploy SSM
---------------------------------------------------------------------------------

SSM supports two running modes, standalone service and SSM service with multiple Smart Agents. If file move performance is not the concern, then standalone service mode is enough. If better performance is desired, we recommend to deploy one agent on each Datanode.
   
   * Standalone SSM Service
   
     For deploy standalone SSM, copy the unzipped tar distribution directory to installation directory (assuming `${SMART_HOME}`) and switch to the directory, the configuration files are under './conf'. 
      
   * SSM Service with multiple Agents
   
     Deploy and unzip tar distribution package to SSM Service server and each Smart Agent server. Smart Agent can coexist with Hadoop HDFS Datanode. For better performance, We recommend to deploy one agent on each Datanode. Of course, Smart Agents on servers other than Datanodes and different numbers of Smart Agents than Datanodes are also supported.
     On the SSM service server, switch to the SSM installation directory, ready to start and run the SSM service. 


Run SSM
---------------------------------------------------------------------------------

* **Format Database**
	
	`./bin/start-ssm.sh -format`

   This command will start SSM service and format database meanwhile.
   The script will drop all tables in the database configured in druid.xml and create all tables required by SSM.
	
   
* **Start SSM server**
   
   SSM server requires HDFS superuser privilege to access some Namenode APIs. So please make sure the account you used to start SSM has the privilege.
   
   The start process also requires the user account to start the SSM server is able to ssh to localhost without providing password.  

   `./bin/start-ssm.sh`

   `--config <config-dir>` can be used to specify where the config directory is.
   `${SMART_HOME}/conf` is the default config directory if the config option is not used.

   If Smart Agents are configured, the start script will start the Agents one by one remotely.
   
   Once you start the SSM server, you can open its web UI by 

   `http://localhost:7045`

   If you meet any problem, please open the smartserver.log under ${SMART_HOME}/logs directory. All the trouble shooting clues are there.

* **Start Smart Agent independently**(optional)

   If you want to add more agents while keeping the SSM service online, you can run the following command on Smart Server.

   `./bin/start-agent.sh [--host .. --config ..]`

   If the host option is not used, localhost is the default one. You should put the hostname specified or localhost in conf/agents.
   So all SSM services can be killed later.

   Please note that the SSM distribution directory should be under the same directory on the new agent host as that on Smart Server.

* **Stop SSM server**
   
   The script `bin/stop-ssm.sh` is used to stop SSM server. Remember to
   use the exact same options to `stop-ssm.sh` script as to `start-ssm.sh` script.
   For example, for starting Smart Server, you use

   `bin/start-ssm.sh --config ./YOUR_CONF_DIR`

   Then, to stop Smart Server, you should use

   `bin/stop-ssm.sh --config ./YOUR_CONF_DIR`
   
   If Smart Agents are configured, the stop script will stop the Agents one by one remotely.


Hadoop Configuration
----------------------------------------------------------------------------------
After install CDH5.10.1 or Hadoop 2.7, please do the following configurations, 

* **Hadoop `core-site.xml`**

    Change property `fs.hdfs.impl` value to point to Smart Server provided "Smart File System".
    
    ```xml
    <property>
        <name>fs.hdfs.impl</name>
        <value>org.smartdata.hadoop.filesystem.SmartFileSystem</value>
        <description>The FileSystem for hdfs URL</description>
    </property>
    ```

* **Hadoop `hdfs-site.xml`**

    Add property `smart.server.rpc.address` to point to the installed Smart Server. Default Smart Server RPC port is `7042`.

    ```xml
    <property>
        <name>smart.server.rpc.address</name>
        <value>ssm-server-ip:port</value>
    </property>   
    ```

     Make sure you have the correct HDFS storage type applied to HDFS DataNode storage volumes, here is an example which sets the SSD, DISK and Archive volumes,

     ```xml
     <property>
         <name>dfs.datanode.data.dir</name>
         <value>[SSD]file:///Users/drankye/workspace/tmp/disk_a,[DISK]file:///Users/drankye/workspace/tmp/disk_b,[ARCHIVE]file:///Users/drankye/workspace/tmp/disk_c</value>
     </property>
     ```

* **Make sure Hadoop HDFS Client can access SSM jars**

    After we switch to the SmartFileSystem from the default HDFS implementation, we need to make sure Hadoop can access SmartFileSystem
implementation jars, so that HDFS, YARN and other upper layer applications can access. There are two ways to ensure Hadoop can access SmartFileSystem, 
	* When SSM compilation is finished, copy all the jar files starts with smart under 

		`/smart-dist/target/smart-data-{version}-SNAPSHOT/smart-data-{version}-SNAPSHOT/lib`

	  to directory in Hadoop/CDH class path.
	  
	 * Add the above path to Hadoop/CDH classpath 


     After all the steps, A cluster restart is required. After the restart, try to run some simple test to see if 
the configuration takes effect. You can try TestDFSIO for example, 

 	* write data
 
	  `hadoop jar ./share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-2.6.0-cdh5.10.1-tests.jar TestDFSIO –write –nrFiles 5 –size 5MB`
   
 	* read data

	  `hadoop jar ./share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-2.6.0-cdh5.10.1-tests.jar TestDFSIO –read`

   You may want to replace the jar with the version used in your cluster. After the read data operation, if all the data files are listed on SSM web UI page "hot files" table, then the integration works very well. 


SSM Rule Examples
---------------------------------------------------------------------------------
* **Move to SSD rule**

	`file: path matches "/test/*" and accessCount(5m) > 3 | allssd`

    This rule means all the files under /test directory, if it is accessed 3 times during
last 5 minutes, SSM should trigger an action to move the file to SSD. Rule engine
will evaluate the condition every MAX{5s,5m/20} internal.


* **Move to Archive(Cold) rule**

	`file: path matches "/test/*" and age > 5h | archive`

    This rule means all the files under /test directory, if it's age is more than 5 hours,
then move the file to archive storage.

* **Move one type of file to specific storage**

	`file: path matches "/test/*.xml" | allssd`

    This rule will move all XML files under /test directory to SSD. In this rule, neither a
single date nor time value is specified, the rule will be evaluated every short time interval (5s by default).

* **Specify rule evaluation interval**

	`file: every 3s | path matches "/test/*.xml" | allssd`
  
    This rule will move all XML files under /test directory to SSD. The rule engine will
evaluate whether the condition meets every 3s. 


* **Backup files between clusters**
     
     `file: every 500ms | path matches "/test-10000-10MB/*"| sync -dest hdfs://sr518:9000/test-10000-10MB/`
	
     This rule will copy file and update any namespace changes(add,delete,rename,append) under source directory "/test-10000-10MB/" to destination directory "hdfs://sr518:9000/test-10000-10MB/". 

* **Support action chain**

	`file: path matches "/test/*" and age > 90d | archive ; setReplica 1 `
	
     SSM use ";" to separate different actions in a rule. The execution trigger of later action depends on the successful execution of the prior action. If prior action fails, the following actions will not be executed.
     
     Above rule means all the files under /test directory, if it's age is more than 90 days, SSM will move the file to archive storage, and set the replica to 1. "setReplica 1" is a not a built-in action. Users need to implement it by themselves.
     
     Please refer to https://github.com/Intel-bigdata/SSM/blob/trunk/docs/support-new-action-guide.md for how to add a new action in SSM.
     
Rule priority and rule order will be considered to implement yet. Currently all rules
will run in parallel. For a full detail rule format definition, please refer to
https://github.com/Intel-bigdata/SSM/blob/trunk/docs/admin-user-guide.md


Performance Tuning
---------------------------------------------------------------------------------
1. Rule and Cmdlet concurrency

   There are two configurable parameters which impact the SSM rule evaluation and action execution parallelism.

    **smart.rule.executors**

    Current default value is 5, which means system will concurrently evaluate 5 rule state at the same time.
    
    ```xml
    <property>
        <name>smart.rule.executors</name>
        <value>5</value>
        <description>Max number of rules that can be executed in parallel</description>
     </property>
     ```

    **smart.cmdlet.executors**

    Current default value is 10, means there will be 10 actions concurrently executed at the same time. 
    If the current configuration cannot meet your performance requirements, you can change it by defining the property in the smart-site.xml under /conf directory. Here is an example to change the action execution parallelism to 50.

     ```xml
     <property>
         <name>smart.cmdlet.executors</name>
         <value>50</value>
     </property>
     ```

2. Cmdlet history purge in metastore  

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

3. Batch Size of Namespace fetcher

    SSM will fetch/sync namespace from namenode when it is started. According to our tests, a large namespace may lead to long start up time. To avoid this, we add a parameter named `smart.namespace.fetcher.batch`, its default value is 500. You can change it if namespace is very large, e.g., 100M or more. A larger batch size will greatly speed up fetcher efficiency, and reduce start up time.

    ```xml
    <property>
        <name>smart.namespace.fetcher.batch</name>
        <value>500</value>
        <description>Batch size of Namespace fetcher</description>
    </property>
    ```

4. Disable SSM Client

    For some reasons, if you do want to disable SmartDFSClients on a specific host from contacting SSM server, it can be realized by creating file "/tmp/SMART_CLIENT_DISABLED_ID_FILE" on that node's local file system. After that, newly created SmartDFSClients on that node will not try to connect SSM server while other functions (like HDFS read/write) will remain unaffected.


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
	 
	 
Notes
---------------------------------------------------------------------------------
1. If there is no SSD and Archive type disk volume configured in DataNodes, actions generated by "allssd" and "archive" rule will fail.
2. When SSM starts, it will pull the whole namespace from Namenode. If the namespace is very huge, it will takes time for SSM to finish the namespace synchronization. SSM may half function during this period.


   
