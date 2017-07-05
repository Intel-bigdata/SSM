# Run/Test SSM with Docker

Docker can greately reduce boring time for installing and maintaining software on servers and developer machines. This document presents this basic workflow of Run/test ssm with docker. [Docker Quick Start](https://docs.docker.com/get-started/)

## MetaStore(mysql) on Docker

### Launch a mysql container

Pull mysql official image from docker store.

```
docker pull mysql
```

Launch a mysql container with a given passowrd on 3306, and create a test database/schema named `{database_name}`.

```bash
docker run -p 3306:3306 --name {container_name} -e MYSQL_ROOT_PASSWORD={root_password} -e MYSQL_DATABASE={database_name} -d mysql:latest
```
Parameters:

- `container_name` name of container
- `root_password` root password of user root for login and access.
-  `database_name` Create a new database/schema with given name.

### Configure MetaStore for SSM

Assuming you are in SSM root directory, modify `conf/druid.xml` to enable SSM to connect with mysql.

```
	<entry key="url">jdbc:mysql://localhost/{database_name}/</entry>
	<entry key="username">root</entry>
	<entry key="password">{root_password}</entry>
```
Use `bin/start-smart.sh -format` to test/re-init the database.

### Stop/Remove Mysql container

You can use the `docker stop {contrainer_name}` to stop mysql container. Then, this mysql service cannot be accessed, until you start it again with `docker start {contrainer_name}`. Note that, `stop/start` will not remove any data from your mysql container.

Use `docker rm {container_name}` to remove mysql container, if this container is not necessary. If you don't remember the specific name of container, you can use `docker ps -a` to look for it.

## More Information

1. [Docker](https://www.docker.com/)
2. [Docker doc](https://docs.docker.com/)
3. [Mysql on Docker](https://store.docker.com/images/mysql)
4. [Docker CN mirror](https://www.docker-cn.com/registry-mirror) 
