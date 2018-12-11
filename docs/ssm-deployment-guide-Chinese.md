[TOC]

# 1. 编译安装包

将编译好的安装包`smart-data-1.5.0-SNAPSHOT.tar.gz`上传到server节点，例如上传到`lhccmh2`的`/opt`目录，并解压：

```
$ cd /opt
$ tar -zxvf smart-data-1.5.0-SNAPSHOT.tar.gz
$ cd smart-data-1.5.0-SNAPSHOT
```

# 2. 配置SSM

## 2.1 配置访问Namenode

生产集群默认开启Namenode-HA模式，下面以启用Namenode高可用场景为例：

编辑`${SMART_HOME}/conf/smart-site.xml`文件，添加以下配置：

```
$ vim conf/smart-site.xml

<property>
    <name>smart.hadoop.conf.path</name>
    <value>/usr/bch/3.0.0/hadoop/etc/hadoop</value>
    <description>Hadoop main cluster configuration file path</description>
</property>
```

> 其中目录`/usr/bch/3.0.0/hadoop/etc/hadoop`包含hdfs的所有配置文件，例如`hdfs-site.xml`、`core-site.xml`。

## 2.2 配置忽略Dirs

默认情况下，SSM启动时将获取整个HDFS命名空间。若不关心某些目录下的文件，那么可添加如下配置来忽略这些文件，并且`rules`也不会触发这些文件相关的`actions`：


编辑`${SMART_HOME}/conf/smart-site.xml`文件，添加以下配置：

```
$ vim conf/smart-site.xml

<property>
    <name>smart.ignore.dirs</name>
    <value>/foodirA,/foodirB</value>
</property>
```

## 2.3 配置Smart Server

SSM支持运行多个Smart Server以获得高可用，其中只有一个Smart Server处于active状态并提供服务，当active的Smart Server失败时，standby的Smart Server将取代它。

编辑`${SMART_HOME}/conf/servers`文件，添加主机名或IP：

```
$ vim conf/servers 
lhccmh2
```
其中active的Smart Server是第一个节点，若启用了HA，故障恢复后，可通过`hdfs dfs -cat /system/ssm.id`命令查看当前active的Smart Server。

为获得更好的性能，可能需要适当调整相应的JVM参数，例如：

编辑`${SMART_HOME}/conf/smart-env.sh`文件，为所有SSM服务（包括Smart Server和Smart Agent）设置最大堆大小：

```
$ vim conf/smart-env.sh
export SSM_JAVA_OPT="-XX:MaxHeapSize=10g"
```
若仅为Smart Server设置最大堆大小，方法如下：

```
$ vim conf/smart-env.sh
export SSM_SERVER_JAVA_OPT="-XX:MaxHeapSize=6g"
```

## 2.4 配置Smart Agent

编辑`${SMART_HOME}/conf/agents`文件，添加主机名或IP：

```
$ vim conf/agents
lhccmh3
lhccmh4
```

若仅为Smart Agent设置最大堆大小，方法如下：

```
$ vim conf/smart-env.sh
export SSM_AGENT_JAVA_OPT="-XX:MaxHeapSize=6g"
```

> 注：Smart Sever需要能免密SSH到上述文件中的所有Smart Agent节点，Smart Agents将会安装到跟Smart Sever一样的目录下。

## 2.5 配置Database

首先需要安装一个MySQL实例，也可采用集群使用的Database，然后编辑`${SMART_HOME}/conf/druid.xml`文件，添加如下配置：

```
<properties>
    <entry key="url">jdbc:mysql://lhccmh2:3306/ssm?useSSL=false</entry>
    <entry key="username">root</entry>
    <entry key="password">root</entry>
</properties>
```
其中`ssm`是数据库名。连接到相应的MySQL后，创建`ssm`数据库并赋相应的权限，步骤大致如下：

```
$ mysql -uroot -proot

mysql> create database ssm;
Query OK, 1 row affected (0.00 sec)

mysql> GRANT ALL PRIVILEGES ON ssm.* TO 'root'@'%' IDENTIFIED BY 'root' WITH GRANT OPTION;
Query OK, 0 rows affected, 1 warning (0.00 sec)

mysql> FLUSH PRIVILEGES;
Query OK, 0 rows affected (0.00 sec)

mysql> use ssm;
Database changed

mysql> show tables;
Empty set (0.00 sec)
```

> 注：推荐使用`MySQL  5.7.18+`版本，否则可能会出现建表索引过长异常：`Specified key was too long; max key length is 767 bytes`。

## 2.6 配置账号访问Web UI

SSM Web UI默认账号密码是`admin/ssm@123`，可编辑`${SMART_HOME}/conf/shiro.ini`文件进行修改：

```
$ vim conf/shiro.ini 
[users]
admin = ssm@123, admin
```

## 2.7 配置Kerberos

若Hadoop集群开启Kerberos，则SSM也需要开启Kerberos以访问安全的集群，编辑`${SMART_HOME}/conf/smart-site.xml`文件，添加以下配置：

```
<property>
    <name>smart.security.enable</name>
    <value>true</value>
</property>
<property>
    <name>smart.server.keytab.file</name>
    <value>/etc/security/keytabs/hdfs.headless.keytab</value>
</property>
<property>
    <name>smart.server.kerberos.principal</name>
    <value>hdfs-ikobe@KOBE.COM</value>
</property>
<property>
    <name>smart.agent.keytab.file</name>
    <value>/etc/security/keytabs/hdfs.headless.keytab</value>
</property>
<property>
    <name>smart.agent.kerberos.principal</name>
    <value>hdfs-ikobe@KOBE.COM</value>
</property>
```

> 注：SSM需要启动用户具有HDFS超级用户权限来访问一些Namenode Api，由于集群开启了Kerberos，为了简便，Smart Server和Smart Agent的keytab文件均采用`hdfs.headless.keytab`，对应的principal为`hdfs`（具有超级用户权限），因此可使用root用户运行SSM。


# 3. 部署SSM

如果需要更好的性能，建议在每个Datanode上部署一个Agent。运行`${SMART_HOME}/bin/install.sh`脚本：

```
$ bin/install.sh
```

> `--config`选项可指定SSM配置目录，若不指定则默认`${SMART_HOME}/conf`。

> 注：Smart Sever需要能免密SSH到所有Smart Agent节点，Smart Agents将会安装到跟Smart Sever一样的目录下。

# 4. 运行SSM

运行SSM服务的先决条件：

1. SSM需要HDFS超级用户权限来访问一些Namenode Api，因此需确保启动SSM的用户账号有此特权。
2. SSM启动过程需要用户能够免密SSH到本地主机和所有Agent节点。

- 查看`hdfs-site.xml`中`dfs.permissions.superusergroup`属性来获取HDFS超级用户组，方法如下：

```
$ cat /usr/bch/3.0.0/hadoop/etc/hadoop/hdfs-site.xml | grep -C 5 'dfs.permissions.superusergroup'
    ...
    <property>
      <name>dfs.permissions.superusergroup</name>
      <value>hdfs</value>
    </property>
```

- 将其他用户添加到HDFS超级用户组方法如下，以smartserver为例：

```
$ vim hdfs-site.xml 
    <property>
      <name>hadoop.user.group.static.mapping.overrides</name>
      <value>smartserver=hdfs;</value>
    </property>
```

## 4.1 启动SSM

运行`${SMART_HOME}/bin/start-ssm.sh`脚本：

```
$ bin/start-ssm.sh -format --config conf/
```

> `-format`选项用于第一次启动SSM时格式化数据库，启动时会删掉`druid.xml`中配置数据库中的所有表，并创建SSM需要的所有表；`--config <config-dir> `选项可指定SSM配置目录，若不指定则默认`${SMART_HOME}/conf`。如果配置了`agents`，该脚本将远程逐个启动Smart Agent。

查看启动进程，如下：

```
$ jps
22691 SmartDaemon
17756 SmartAgent
```

SSM启动成功后，可通过以下WEB UI访问：

```
http://Active_SSM_Server_IP:7045
```

## 4.2 停止SSM

运行`${SMART_HOME}/bin/stop-ssm.sh`脚本：

```
$ bin/stop-ssm.sh
```

# 5. 测试SSM EC

- 查看当前Hadoop集群的EC策略，默认开启`RS-6-3-1024k`：

```
$ hdfs ec -listPolicies
Erasure Coding Policies:
ErasureCodingPolicy=[Name=RS-10-4-1024k, Schema=[ECSchema=[Codec=rs, numDataUnits=10, numParityUnits=4]], CellSize=1048576, Id=5], State=DISABLED
ErasureCodingPolicy=[Name=RS-3-2-1024k, Schema=[ECSchema=[Codec=rs, numDataUnits=3, numParityUnits=2]], CellSize=1048576, Id=2], State=DISABLED
ErasureCodingPolicy=[Name=RS-6-3-1024k, Schema=[ECSchema=[Codec=rs, numDataUnits=6, numParityUnits=3]], CellSize=1048576, Id=1], State=ENABLED
ErasureCodingPolicy=[Name=RS-LEGACY-6-3-1024k, Schema=[ECSchema=[Codec=rs-legacy, numDataUnits=6, numParityUnits=3]], CellSize=1048576, Id=3], State=DISABLED
ErasureCodingPolicy=[Name=XOR-2-1-1024k, Schema=[ECSchema=[Codec=xor, numDataUnits=2, numParityUnits=1]], CellSize=1048576, Id=4], State=DISABLED
```

- 启用`RS-3-2-1024k`策略：

```
$ hdfs ec -enablePolicy -policy RS-3-2-1024k
Erasure coding policy RS-3-2-1024k is enabled
```

- 创建测试文件：

```
$ hdfs dfs -mkdir -p /user/ssm
$ hdfs dfs -copyFromLocal autodeploy-hc.sh /user/ssm
$ hdfs ec -getPolicy -path /user/ssm/autodeploy-hc.sh
The erasure coding policy of /user/ssm/autodeploy-hc.sh is unspecified
```


## 5.1 添加Rules

### 5.1.1 提交Rule

![image](https://note.youdao.com/yws/api/personal/file/E68FD7EA4092418BA0EDBB0C43F3F190?method=download&shareKey=1c746748a02647577eee2813c7218d4f)

Rule为：

```
file: path matches "/user/ssm/*.sh" and age > 10min | ec -policy RS-3-2-1024k
```

### 5.1.2 开启Rule

![image](https://note.youdao.com/yws/api/personal/file/5F9AB289A70B4F14B91A79CD1C5FBB37?method=download&shareKey=b7a540a2a009adeaec755507e091c76e)

Rule Enable后状态由`DISABLED`变为`ACTIVE`，如下：

![image](https://note.youdao.com/yws/api/personal/file/137500F0E9B54EE9AC5367F72BBE7B17?method=download&shareKey=f10a66fba97818885f41c100fe344a9c)

### 5.1.3 Rule触发Action

![image](https://note.youdao.com/yws/api/personal/file/AAB2F8A254DE41659EE558F7815BEF9E?method=download&shareKey=0d68c60708cbf98ae7456b5b3aa81560)

运行结果为：

![image](https://note.youdao.com/yws/api/personal/file/6870BFED1E8346E9BCA31E40A87BEABD?method=download&shareKey=a7dceb885dced7f16809b735b29d67e5)

后台核实该文件策略：

```
$ hdfs ec -getPolicy -path /user/ssm/autodeploy-hc.sh
RS-3-2-1024k
$ hdfs fsck /user/ssm/autodeploy-hc.sh -files -blocks -locations
Connecting to namenode via http://lhccmh3:50070/fsck?ugi=hdfs&files=1&blocks=1&locations=1&path=%2Fuser%2Fssm%2Fautodeploy-hc.sh
FSCK started by hdfs (auth:KERBEROS_SSL) from /10.139.17.59 for path /user/ssm/autodeploy-hc.sh at Thu Dec 06 14:58:11 CST 2018
/user/ssm/autodeploy-hc.sh 2057 bytes, erasure-coded: policy=RS-3-2-1024k, 1 block(s):  OK
0. BP-2104048606-10.139.17.60-1542612977908:blk_-9223372036854775536_4409 len=2057 Live_repl=3  [blk_-9223372036854775536:DatanodeInfoWithStorage[10.139.17.61:1019,DS-494836d4-12ef-4ea8-808e-b2384ae231bb,DISK], blk_-9223372036854775533:DatanodeInfoWithStorage[10.139.17.127:1019,DS-18738899-bdb5-4fb3-a9db-5f9ff729ba86,DISK], blk_-9223372036854775532:DatanodeInfoWithStorage[10.139.17.119:1019,DS-1a9abde8-2d99-4824-b5a0-542a9367c597,DISK]]
...
```

取消EC策略的Rule可设置如下：

```
file: path matches "/user/ssm/*.sh" and age > 10min | unec
```


## 5.2 添加Action

### 5.2.1 运行Action

![image](https://note.youdao.com/yws/api/personal/file/19042C0298BE43BA9744E1113325C254?method=download&shareKey=8b1839954f378b304d7ccf42d07f77db)

### 5.2.2 查看Action运行状态

![image](https://note.youdao.com/yws/api/personal/file/4AC86119B24540B78851F90B67D5A7FA?method=download&shareKey=b1522ffa478fd2b5310e279ce7f12687)

### 5.2.3 查看Action运行结果

![image](https://note.youdao.com/yws/api/personal/file/E111DF607C2A410BA21D3096276C4F7E?method=download&shareKey=bece41c2083c0a9f6bf6391a5f212066)


参考网址：
* https://github.com/Intel-bigdata/SSM/blob/trunk/docs/ssm-deployment-guide.md
* https://github.com/Intel-bigdata/SSM/blob/trunk/docs/enable-kerberos.md
* https://github.com/Intel-bigdata/SSM/blob/trunk/docs/admin-user-guide.md
