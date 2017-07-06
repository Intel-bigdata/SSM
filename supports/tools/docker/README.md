# Run/Test SSM with Docker

Docker can greately reduce boring time for installing and maintaining software on servers and developer machines. This document presents this basic workflow of Run/test ssm with docker. [Docker Quick Start](https://docs.docker.com/get-started/)


## Necessary Components

### MetaStore(mysql) on Docker

#### Launch a mysql container

Pull latest mysql official image from docker store. You can use `mysql:tag` to specify the MySQL version (`tag`) you want.

```
docker pull mysql
```

Launch a mysql container with a given passowrd on 3306, and create a test database/schema named `{database_name}`.

```bash
docker run -p 3306:3306 --name {container_name} -e MYSQL_ROOT_PASSWORD={root_password} -e MYSQL_DATABASE={database_name} -d mysql:latest
```
**Parameters:**

- `container_name` name of container
- `root_password` root password of user root for login and access.
-  `database_name` Create a new database/schema with given name.

### HDFS on Docker
Pull a well-known third-party hadoop image from docker store. You can use `hadoop-docker:tag` to specify the Hadoop version (`tag`) you want.

```bash
docker pull sequenceiq/hadoop-docker
```

Launch a Hadoop container with a exposed namenode.rpcserver.

```bash
docker run -it --add-host=moby:127.0.0.1 -p 9000:9000 --name=hadoop sequenceiq/hadoop-docker /etc/bootstrap.sh -bash
```
Note that we try to launch a interactive docker container. Use the following command to check HDFS status.

```
cd $HADOOP_PREFIX
bin/hdfs dfs -cat output/*
```
Edit `$HADOOP_PREFIX/etc/hadoop/hdfs-site.xml` and add the property below. This will turn off premission check to avoid `Access denied for user ***. Superuser privilege is required`.

```xml
<property>
    <name>dfs.permissions.enabled</name>
    <value>false</value>
</property>
```
Restart HDFS.
```
$HADOOP_PREFIX/sbin/stop-dfs.sh
$HADOOP_PREFIX/sbin/start-dfs.sh
```

## SSM Configuration

### MetaStore

#### Configure MetaStore for SSM

Assuming you are in SSM root directory, modify `conf/druid.xml` to enable SSM to connect with mysql.

```
	<entry key="url">jdbc:mysql://localhost/{database_name}</entry>
	<entry key="username">root</entry>
	<entry key="password">{root_password}</entry>
```
Wait for at least 10 seconds. Then, use `bin/start-smart.sh -format` to format (re-init) the database. Also, you can use this command to clear all data in database in tests.

#### Stop/Remove Mysql container

You can use the `docker stop {contrainer_name}` to stop mysql container. Then, this mysql service cannot be accessed, until you start it again with `docker start {contrainer_name}`. Note that, `stop/start` will not remove any data from your mysql container.

Use `docker rm {container_name}` to remove mysql container, if this container is not necessary. If you don't remember the specific name of container, you can use `docker ps -a` to look for it.


### HDFS

### Configure HDFS for SSM

Configure `namenode.rpcserver` in `smart-site.xml`.

```xml
<configuration>
    <property>
        <name>smart.dfs.namenode.rpcserver</name>
        <value>hdfs://localhost:9000</value>
        <description>Namenode rpcserver</description>
    </property>
</configuration>
```

## More Information

1. [Docker](https://www.docker.com/)
2. [Docker doc](https://docs.docker.com/)
3. [Mysql on Docker](https://store.docker.com/images/mysql)
4. [Docker CN mirror](https://www.docker-cn.com/registry-mirror) 
