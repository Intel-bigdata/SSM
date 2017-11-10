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
      <value>hdfs://namenode-ip:9000</value>
      <description>Hadoop cluster Namenode RPC server address and port</description>
   </property>
   ```
 
* **Configure Smart Agent (optional)**

   This step can be skipped if SSM standalone mode is preferred.
  
   Open `agents` file under /conf , put each Smart Agent server's hostname or IP address line by line. This configuration file is required by Smart Server to communicate with each Agent. So please make sure Smart Server can access these hosts by SSH without password.
   After the configuration, the Smart Agents should be installed in the same path on their respective hosts as the one of Smart Server.
 
* **Configure database**

   SSM currenlty supports MySQL and TiDB(rc vesion) as the backend to store metadata for SSM. TiDB is a distributed NewSQL database, which can provide good scalability and high availability for SSM.

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

    Since TiDB has been integrated with SSM, you do not need to install TiDB beforehand.
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
    So the storage capacity of SSM-TiDB can easily be scaled up by just adding more agent server. This is a great advantage over MySQL.

    If TiDB is enabled, the jdbc url should be the following default one. The configuration in druid.xml is shown as follows.

   ```xml
    <properties>
        <entry key="url">jdbc:mysql://127.0.0.1:4000/test</entry>
        <entry key="username">root</entry>
        <entry key="password"></entry>
        ......
    <properties>
   ```

    TiDB supports the usage of MySQL shell. The way of MySQL shell connecting to TiDB server is as same as that for MySQL.
    The default command to enter into MySQL shell on Smart Server is `mysql -h 127.0.0.1 -u root -P 4000`.

* **Configure user account to authenticate to Web UI**

    By default, SSM Web UI enables user login with default user "admin", password "ssm@123".  If user wants to change the password to define more user accounts, go to the conf/shiro.int file, 
    
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

SSM supports two running modes, standalone service and SSM service with multiple Smart Agents. If file move performance is not the concern, then standalone servie mode is enough. If better performance is desired, we recommend to deploy one agent on each Datanode.
   
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

   If you meet any problem, please open the smartserver.log under /logs directory. All the trouble shooting clues are there.
   
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

    Add property `smart.server.rpc.adddress` and `smart.server.rpc.port` to point to installed Smart Server.

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

     Make sure you have the correct HDFS storage type applied to HDFS DataNode storage volumes, here is an example which sets the SSD, DISK and Archive volumes,

     ```xml
     <property>
         <name>dfs.datanode.data.dir</name>
         <value>[SSD]file:///Users/drankye/workspace/tmp/disk_a,[DISK]file:///Users/drankye/workspace/tmp/disk_b,[ARCHIVE]file:///Users/drankye/workspace/tmp/disk_c</value>
     </property>
     ```

* **Make sure Hadoop HDFS Client can access SSM jars**

    After we switch to the SmartFileSystem from the default HDFS implmentation, we need to make sure Hadoop can access SmartFileSystem
implementation jars, so that HDFS, YARN and other upper layer applications can access. There are two ways to ensure Hadoop can access SmartFileSystem, 
	* When SSM compilation is finished, copy all the jar files starts with smart under 

		`/smart-dist/target/smart-data-{version}-SNAPSHOT/smart-data-{vesion}-SNAPSHOT/lib`

	  to directory in Hadoop/CDH class path.
	  
	 * Add the above path to Hadoop/CDH classpath 


     After all the steps, A cluster restart is required. After the restart, try to run some simple test to see if 
the configuration takes effect. You can try TestDFSIO for example, 

 	* write data
 
	  `hadoop jar ./share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-2.6.0-cdh5.10.1-tests.jar TestDFSIO –write –nrFiles 5 –size 5MB`
   
 	* read data

	  `hadoop jar ./share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-2.6.0-cdh5.10.1-tests.jar TestDFSIO –read`

   You may want to replace the jar with the version used in your cluster. After the read data opertion, if all the data files are listed on SSM web UI page "hot files" table, then the integration works very well. 


SSM Rule Examples
---------------------------------------------------------------------------------
* **Move to SSD rule**

	`file: path matches "/test/*" and accessCount(5m) > 3 | allssd`

    This rule means all the files under /test directory, if it is accessed 3 times during
last 5 minutes, SSM should trigger an anction to move the file to SSD. Rule engine
will evalue the condition every MAX{5s,5m/20} internal.


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

Rule priority and rule order will be considered to implement yet. Currenlty all rules
will run parallelly. For a full detail rule format definition, please refer to 
https://github.com/Intel-bigdata/SSM/blob/trunk/docs/admin-user-guide.md


Performance Tunning
---------------------------------------------------------------------------------
There are two configurable parameters which impact the SSM rule evalute and action 
execution parallism.

**smart.rule.executors**

Current default value is 5, which means system will concurrently evalue 5 rule state 
at the same time.

**smart.cmdlet.executors**

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


   
