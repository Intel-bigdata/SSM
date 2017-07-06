## Necessary Components

### HDFS
Assueme HDFS is correctly deployed, and its namenode is available on `{namenode.rpcserver:port}`. Please ensure that SSM can visit namenode.

### MetaStore
SSM metastore stores the metadata of SSM. Currently, we uses mysql as its metastore. Please ensure that SSM can access to mysql.

## SSM

### HDFS
Configure `namenode.rpcserver` in `smart-site.xml`.

```xml
<configuration>
    <property>
        <name>smart.dfs.namenode.rpcserver</name>
        <value>hdfs://{namenode.rpcserver:port}</value>
        <description>Namenode rpcserver</description>
    </property>
</configuration>
```

**Parameters:**

- `{namenode.rpcserver:port}` is the ip address:port of namenode.rpcserver.


### Metasotre
`druid.xml`

```xml
    <entry key="url">jdbc:mysql://{mysql_address}/{database_name}</entry>
    <entry key="username">{username}</entry>
    <entry key="password">{password}</entry>
```

**Parameters:**

- `{mysql_address:port}` is the ip address:port of mysql server.
- `{database_name}` is the database used by SSM. Please create this database before launching SSM.
- `{username}`/`{password}` username and password for connection mysql.
