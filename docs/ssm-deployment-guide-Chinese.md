# SSM配置与部署

# 简介

Intel<sup>®</sup> Smart Storage Management (SSM) 项目致力于提供针对HDFS数据的智能管理方案。SSM有如下几个重要的功能。

- SSM mover，用于冷热数据分层管理。根据用户定制的SSM规则，SSM区分出冷热数据，将冷热数据分别迁移到合适的存储介质上，合理利用不同的存储设备。
- SSM小文件优化，能将HDFS小文件合并成一个大文件，在合并后，仍然支持对小文件透明的读操作。
- SSM data sync，能够将一个HDFS集群中的数据自动同步到另一个集群上。
- SSM EC (Erasure Coding)，可将HDFS数据转化成某个EC策略下的数据，能够显著减少占用的存储空间。
- SSM compression，可按照指定的压缩算法，将HDFS文件压缩。

项目代码链接：https://github.com/Intel-bigdata/SSM/

本文概括了SSM的基本配置与部署步骤。

# 1. 编译安装包

从SSM代码仓库下载SSM源码，针对不同的Hadoop版本，采用如下不同的编译命令。

#### CDH 5.10.1

    mvn clean package -Pdist,web,hadoop-cdh-2.6 -DskipTests

#### Hadoop 2.7.3

    mvn clean package -Pdist,web,hadoop-2.7 -DskipTests

#### Hadoop 3.1.0

    mvn clean package -Pdist,web,hadoop-3.1 -DskipTests

编译好的安装包smart-data-*.tar.gz位于SSM/smart-dist/target下，可将其传到server节点，并解压，得到smart-data-*为${SMART_HOME}

# 2. 配置SSM

## 2.1 配置访问Namenode

1) 生产集群HDFS通常开启namenode-HA模式，该场景下配置SSM如下：
   编辑${SMART_HOME}/conf/smart-site.xml文件，配置Hadoop的conf目录，如下：

    ```xml
    <property>
        <name>smart.hadoop.conf.path</name>
        <value>/root/hadoop-3.1.0/etc/hadoop</value>
        <description>Hadoop main cluster configuration file path</description>
    </property>
    ```

> 其中目录`/root/hadoop-3.1.0/etc/hadoop`包含hdfs的所有配置文件，例如`hdfs-site.xml`、`core-site.xml`等。

2) 如果HDFS集群开启了Kerberos认证，也要采用上面的配置。
3) 如果HDFS集群没有开启namenode-HA及Kerberos认证，可只配置namenode rpc地址，如下所示：
   ```xml
    <property>
        <name>smart.dfs.namenode.rpcserver</name>
        <value>hdfs://node1:9000</value>
    </property>
    ```

## 2.2 配置忽略HDFS数据目录 \[可选]

默认情况下，SSM启动时将获取整个HDFS命名空间。若不关心某些目录下的文件，那么可修改如下配置来忽略这些文件，并且提交的rule也不会触发与这些文件相关的actions：

编辑${SMART_HOME}/conf/smart-default.xml文件，以忽略/foo-dirA，/foo-dirB为例，修改如下：

```xml
<property>
    <name>smart.ignore.dirs</name>
    <value>/foo-dirA,/foo-dirB</value>
</property>
```

## 2.3 配置Smart Server \[可选]

SSM支持运行一个或多个Smart Server。多个Smart Server用来保证HA，其中只有一个Smart Server处于active状态并提供相应服务，当active Smart Server失败时，standby Smart Server将变成active状态。
* SSM默认只配置了一个Smart Server，即localhost，如需SSM HA模式，可操作如下。

    编辑`${SMART_HOME}/conf/servers`文件，添加主机名或IP：

    ```
    node1
    node2
    ```
    其中active的Smart Server是第一个节点，若启用了HA，故障恢复后，可通过`hdfs dfs -cat /system/ssm.id`命令查看当前active的Smart Server。

* 为获得更好的性能，可能需要适当调整相应的JVM参数，例如：

    编辑`${SMART_HOME}/conf/smart-env.sh`文件，为所有SSM服务（包括Smart Server和Smart Agent）设置最大堆大小：

    ```
    export SSM_JAVA_OPT="-XX:MaxHeapSize=10g"
    ```
    若仅为Smart Server设置最大堆大小，方法如下：

    ```
    export SSM_SERVER_JAVA_OPT="-XX:MaxHeapSize=6g"
    ```

## 2.4 配置Smart Agent \[可选]

* 编辑`${SMART_HOME}/conf/agents`文件，添加主机名或IP：

```
    node3
    node4
```

* 若仅为Smart Agent设置最大堆大小，方法如下：

```
    export SSM_AGENT_JAVA_OPT="-XX:MaxHeapSize=6g"
```

> 注：Smart Agent可以同Smart Server部署到一个节点上。

## 2.5 配置Database

SSM需要MySQL来存储元数据，用户需要部署一个MySQL实例，然后编辑${SMART_HOME}/conf/druid.xml文件，配置示例如下：

```xml
<properties>
    <entry key="url">jdbc:mysql://node1:3306/ssm?useSSL=false</entry>
    <entry key="username">root</entry>
    <entry key="password">123456</entry>
</properties>
```
其中`ssm`是数据库名,需要用户创建。

> 注：推荐使用`MySQL  5.7.18+`版本，否则可能会报建表索引过长的异常.

## 2.6 配置账号访问Web UI \[可选]

SSM Web UI默认账号密码是`admin/ssm@123`。 如有需要，可编辑`${SMART_HOME}/conf/shiro.ini`文件进行修改：

```
[users]
admin = ssm@123, admin
```

## 2.7 配置Kerberos \[可选]

若Hadoop集群开启Kerberos，则SSM也需要开启Kerberos以访问安全的集群，编辑`${SMART_HOME}/conf/smart-site.xml`文件，添加以下配置：

```xml
<property>
    <name>smart.security.enable</name>
    <value>true</value>
</property>
<property>
    <name>smart.server.keytab.file</name>
    <value>/etc/security/keytabs/hdfs.keytab</value>
</property>
<property>
    <name>smart.server.kerberos.principal</name>
    <value>hdfs@HADOOP.COM</value>
</property>
<property>
    <name>smart.agent.keytab.file</name>
    <value>/etc/security/keytabs/hdfs.keytab</value>
</property>
<property>
    <name>smart.agent.kerberos.principal</name>
    <value>hdfs@HADOOP.COM</value>
</property>
```

> 注：SSM需要启动用户具有HDFS超级用户权限来访问一些HDFS namenode api，由于集群开启了Kerberos，为了简便，Smart Server和Smart Agent的keytab文件均采用`hdfs.keytab`，对应的principal为`hdfs`（具有超级用户权限），因此可使用root用户运行SSM。

## 2.8 权限配置 \[可选]

* SSM需要HDFS超级用户权限来访问一些Namenode Api，因此需确保启动SSM的用户账号有此特权。

    查看`hdfs-site.xml`中`dfs.permissions.superusergroup`属性来获取HDFS超级用户组，看到的配置如下：

    ```xml
    <property>
        <name>dfs.permissions.superusergroup</name>
        <value>hdfs</value>
    </property>
    ```

    将其他用户添加到HDFS超级用户组方法如下，以smartserver用户为例：

    ```xml
    <property>
        <name>hadoop.user.group.static.mapping.overrides</name>
        <value>smartserver=hdfs;</value>
    </property>
    ```

* SSM启动过程需要用户能够免密SSH到本地主机和所有Agent节点。

# 3. 部署SSM

只要配置一个Smart Server，SSM就可以工作。但如果追求更好的性能，建议在每个datanode上部署一个Smart Agent。

运行`${SMART_HOME}/bin/install.sh`脚本：

```
$ bin/install.sh
```

> `--config`选项可指定SSM配置目录，若不指定则默认`${SMART_HOME}/conf`。

> 注：Smart Sever需要能免密SSH到所有Smart Agent节点，Smart Agents将会安装到同Smart Sever一样的路径下。

# 4. 运行SSM

## 4.1 启动SSM

运行`${SMART_HOME}/bin/start-ssm.sh`脚本：

```
$ bin/start-ssm.sh
```

> `-format`选项用于启动SSM时格式化数据库，会删掉`druid.xml`中配置数据库中的所有表，并创建SSM需要的所有表；
`--config <config-dir> `选项可指定SSM配置目录，若不指定则默认`${SMART_HOME}/conf`。

SSM启动成功后，可通过以下WEB UI访问：

```
http://Active_SSM_Server_IP:7045
```

如启动失败,可通过查看${SMART_HOME}/log下面的日志文件,获取错误信息来排查问题。

## 4.2 停止SSM

运行`${SMART_HOME}/bin/stop-ssm.sh`脚本：

```
$ bin/stop-ssm.sh
```


参考文档：
* https://github.com/Intel-bigdata/SSM/blob/trunk/docs/ssm-deployment-guide.md
* https://github.com/Intel-bigdata/SSM/blob/trunk/docs/enable-kerberos.md
* https://github.com/Intel-bigdata/SSM/blob/trunk/docs/admin-user-guide.md
