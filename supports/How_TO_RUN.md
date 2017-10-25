## Necessary Components

### HDFS
Assume HDFS is correctly deployed, and its namenode is available on `{namenode.rpcserver:port}`. Please make sure that SSM can connect to namenode.

### MetaStore
SSM need a metstore to store the metadata of SSM. Currently, we uses mysql as its metastore. Please ensure that mysql is correctly installed, and SSM can access to mysql.

## SSM Configurations

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

- `{namenode.rpcserver:port}` is the ip `address:port` of `namenode.rpcserver`.


### Metasotre
`druid.xml`

```xml
    <entry key="url">jdbc:mysql://{mysql_address}/{database_name}</entry>
    <entry key="username">{username}</entry>
    <entry key="password">{password}</entry>
```

**Parameters:**

- `{mysql_address:port}` is the ip address:port of mysql server. If you want to redirect mysql warning/error log to logs dir. Please append `?logger=com.mysql.jdbc.log.Slf4JLogger` to url.
- `{database_name}` is the database used by SSM. **Please create this database before launching SSM.**
- `{username}`/`{password}` username and password for connection mysql.

**Note that you should initialize the database before launching SSM.*** Use `./bin/start-smart.sh -format` to initialize the database.


## Troubleshooting

1. HDFS Exceptions

Please check the network between SSM and namenode/datanodes. Then, check if SSM have enough permission to manage HDFS.

2. MetaStore Exceptions

Please check if the database is correctly set up. Check if the required database is correctly created and initialized.

