## Kerberos Enabling Guide

### 1. Deploy Kerberos KDC

#### 1.1 Install Kerberos

```
yum install -y krb5-server krb5-lib krb5-workstation
```

#### 1.2 Config kdc.conf
Exmaple:
```
[kdcdefaults]
  kdc_ports = 88
  kdc_tcp_ports = 88
 
 [realms]
  HADOOP.COM = {
   master_key_type = aes128-cts
   acl_file = /var/kerberos/krb5kdc/kadm5.acl
   dict_file = /usr/share/dict/words
   admin_keytab = /var/kerberos/krb5kdc/kadm5.keytab
   max_renewable_life = 7d
   supported_enctypes = aes128-cts:normal des3-hmac-sha1:normal arcfour-hmac:normal des-hmac-sha1:normal des-cbc-md5:normal des-cbc-crc:normal
  }
```
Set KRB5_KDC_PROFILE environment variables to point to the kdc.conf. For example:
```
export KRB5_KDC_PROFILE=/yourdir/kdc.conf
```


#### 1.3 Config krb5.conf
Example:
```
 [libdefaults]
  default_realm = HADOOP.COM
  dns_lookup_realm = false
  dns_lookup_kdc = false
  ticket_lifetime = 24h
  renew_lifetime = 7d
  max_life = 12h 0m 0s
  forwardable = true
  udp_preference_limit = 1
 
 [realms]
  HADOOP.COM = {
   kdc = hadoop1:88
   admin_server = hadoop1:749
   default_domain = HADOOP.COM
  }
 
 [logging]
  default = FILE:/var/log/krb5libs.log
  kdc = FILE:/var/log/krb5kdc.log
  admin_server = FILE:/var/log/kadmind.log
```
Set KRB5_CONFIG environment variables to point to the krb5.conf. For example:
```
export KRB5_CONFIG=/yourdir/krb5.conf
```

#### 1.4 Create the KDC database
The following is an example of how to create a Kerberos database and stash file on the master KDC, using the kdb5_util command. Replace HADOOP.COM with the name of your Kerberos realm.
```
kdb5_util create -r HADOOP.COM -s
```
This will create five files in LOCALSTATEDIR/krb5kdc (or at the locations specified in kdc.conf):

#### 1.5 Start Kerberos daemons on master KDC
```
krb5kdc
kadmind
```

### 2. Export keytabs

#### 2.1 Add smart server (standby server) Kerberos principal to database and export it to keytab.
```
kadmin.local:addprinc -randkey {username}/{hostname}
kadmin.local:xst -k /xxx/xxx/smartserver-{hostname}.keytab {username}/{hostname}
```
**Note:** replace the username with the HDFS user who has the correct permission to execute actions and replace hostname with hostname of the smart server

#### 2.2 Add smart agent Kerberos principals to database and export it to keytabs.
Please create principals for each agent. Then, use 'scp' to copy each keytab file to the corresponding agent node.
```
kadmin.local:addprinc -randkey {username}/{hostname}
kadmin.local:xst -k /xxx/xxx/smartagent-{hostname}.keytab {username}/{hostname}
scp /xxx/xxx/smartagent-{hostname}.keytab {hostname}:/xxx/xxx/
```

#### 2.3 Add hdfs Kerberos principals to database and export it to keytabs.
```
kadmin.local:addprinc -randkey hdfs/{hostname}
kadmin.local:xst -k /xxx/xxx/hdfs-{hostname}.keytab hdfs/{hostname}
```

> Note: please replace {hostname} with the real hostname. The below section should follow the same convention.

### 3. Configure SSM

Please update smart-site.xml for each node. The principal and keytab path can be different for each agent node.
If multiple smart servers are configured, the principal and keytab path should be configured respectively for
each smart server.

```xml
<!-- hadoop conf dir should be configured -->
<property>
    <name>smart.hadoop.conf.path</name>
    <value>/xxx/xxx/etc/hadoop</value>
    <description>Hadoop main cluster configuration file path</description>
</property>

<property>
    <name>smart.security.enable</name>
    <value>true</value>
</property>
<property>
    <name>smart.server.keytab.file</name>
    <value>/xxx/xxx/smartserver-{hostname}.keytab</value>
</property>
<property>
    <name>smart.server.kerberos.principal</name>
    <value>{username}/_HOST@HADOOP.COM</value>
</property>
<property>
    <name>smart.agent.keytab.file</name>
    <value>/xxx/xxx/smartagent-{hostname}.keytab</value>
</property>
<property>
    <name>smart.agent.kerberos.principal</name>
    <value>{username}/_HOST@HADOOP.COM</value>
</property>
```

> Note: _HOST will be converted to the real hostname by program. But if it is not correctly mapped to the hostname you used for adding principle,
_HOST should be replaced by the expected hostname. This convention should also be followed in the below sections.

### 4. Configure HDFS
 
#### 4.1 Update core-site.xml
Add the following properties:
```xml
<property>
    <name>hadoop.security.authorization</name>
    <value>true</value>
</property>
<property>
    <name>hadoop.security.authentication</name>
    <value>kerberos</value>
</property>
```

#### 4.2 Update hdfs-site.xml
Add the following properties:
```xml
<!-- General HDFS security config -->
<property>
    <name>dfs.block.access.token.enable</name>
    <value>true</value>
</property>

<!-- NameNode security config -->
<property>
    <name>dfs.namenode.keytab.file</name>
    <value>/xxx/xxx/hdfs-{hostname}.keytab</value>
</property>
<property>
    <name>dfs.namenode.kerberos.principal</name>
    <value>hdfs/_HOST@HADOOP.COM</value>
</property>
<property>
    <name>dfs.namenode.delegation.token.max-lifetime</name>
    <value>604800000</value>
    <description>The maximum lifetime in milliseconds for which a delegation token is valid.</description>
</property>

<!-- Configuration for HDFS web on namenode [optional] -->
<property>
    <name>dfs.webhdfs.enabled</name>
    <value>true</value>
</property>
<property>
    <name>dfs.web.authentication.kerberos.principal</name>
    <value>web/_HOST@HADOOP.COM</value>
</property>
<property>
    <name>dfs.web.authentication.kerberos.keytab</name>
    <value>/root/sorttest/web-{hostname}.keytab</value>
</property>

<!-- To fix resolving hostname problem [optional] -->
<property>
  <name>dfs.namenode.datanode.registration.ip-hostname-check</name>
  <value>false</value>
</property>
<property>
  <name>dfs.client.use.datanode.hostname</name>
  <value>false</value>
</property>
<property>
  <name>dfs.datanode.use.datanode.hostname</name>
  <value>false</value>
</property>

<!-- Configuration for datanode security, should not be added on namenode -->
<property>
    <name>dfs.datanode.data.dir.perm</name>
    <value>700</value>
</property>
<property>
    <name>dfs.datanode.keytab.file</name>
    <value>/xxx/xxx/hdfs-{hostname}.keytab</value>
</property>
<property>
    <name>dfs.datanode.kerberos.principal</name>
    <value>hdfs/_HOST@HADOOP.COM</value>
</property>

<!-- Config for SmartClient, if SmartFileSystem is configured for HDFS,
we should make the below config for all HDFS (or other HDFS application)
cluster nodes. -->
<property>
    <name>smart.server.kerberos.principal</name>
    <value>{username}/{smart_server_host}@HADOOP.COM</value>
</property>
```

### 5. Install jsvc

You can configure jsvc instead of HTTPS through the following steps.

#### 5.1 Install jsvc

jsvc should be installed on each datanode.

`sudo yum install jsvc` or `apt-get install jsvc`

Please add the following statements in hadoop-env.sh for hadoop.

`export HADOOP_SECURE_DN_USER="root"`

`export JSVC_HOME=/usr/bin`

#### 5.2 Additional modification in hdfs-site.xml
```xml
<property>
    <name>dfs.datanode.address</name>
    <value>0.0.0.0:1004</value>
</property>
<property>
    <name>dfs.datanode.http.address</name>
    <value>0.0.0.0:1006</value>
</property>
```

### 6. Enable HTTPS [Alternative]

**Please ignore this step if 5th step is used.**

You can configure HTTPS instead of jsvc through the following steps.

#### 6.1 Create a keystore file for each host

> keystore: the keystore file that stores the certificate.
> validity: the valid time of the certificate in days.
```
keytool -alias {hostname} -keystore {keystore} -validity {validity} -genkey
```

> The keytool will ask for more details such as the keystore password, keypassword and CN(hostname).

#### 6.2 Export the certificate public key to a certificate file for each host
```
keytool -export -alias {hostname} -keystore {keystore} -rfc -file {cert-file}
```

#### 6.3 Create a common truststore file (trustAll)
The truststore file contains the public key from all certificates. If you assume a 2-node cluster with node1 and node2,
login to node1 and import the truststore file for node1.
```
keytool -import -alias {hostname} -keystore {trustAll} -file {cert-file}
```

#### 6.4 Update the common truststore file
* Move {trustAll} from node1 to node2 ({trustAll} already has the certificate entry of node1), and repeat Step 3.

* Move the updated {trustAll} from node2 to node1. Repeat these steps for each node in the cluster.
When you finish, the {trustAll} file will have the certificates from all nodes.

> Note: these work could be done on the same node, just notice the hostname.

#### 6.5 Copy {trustAll} from node1 to all of the other nodes

#### 6.6. Validate the common truststore file
```
keytool -list -v -keystore {trustAll}
```

#### 6.7. Edit the Configuration files

> Config $HADOOP_HOME/etc/hadoop/ssl-server.xml for Hadoop
```xml
<property>
    <name>ssl.server.truststore.location</name>
    <value>path to trustAll</value>
</property>
<property>
    <name>ssl.server.truststore.password</name>
    <value>trustAll password</value>
</property>
<property>
    <name>ssl.server.truststore.type</name>
    <value>jks</value>
</property>
<property>
    <name>ssl.server.truststore.reload.interval</name>
    <value>10000</value>
</property>
<property>
    <name>ssl.server.keystore.location</name>
    <value>path to keystore</value>
</property>
<property>
    <name>ssl.server.keystore.password</name>
    <value>keystore password</value>
</property>
<property>
    <name>ssl.server.keystore.keypassword</name>
    <value>keystore keypassword</value>
</property>
<property>
    <name>ssl.server.keystore.type</name>
    <value>jks</value>
</property>
```

> Config $HADOOP_HOME/etc/hadoop/ssl-client.xml for Hadoop
```xml
<property>
    <name>ssl.client.truststore.location</name>
    <value>patch to trustAll</value>
</property>

<property>
    <name>ssl.client.truststore.password</name>
    <value>trustAll password</value>
</property>
<property>
    <name>ssl.client.truststore.type</name>
    <value>jks</value>
</property>
<property>
    <name>ssl.client.truststore.reload.interval</name>
    <value>10000</value>
</property>
<property>
    <name>ssl.client.keystore.location</name>
    <value>path to keystore</value>
</property>
<property>
    <name>ssl.client.keystore.password</name>
    <value>keystore password</value>
</property>
<property>
    <name>ssl.client.keystore.keypassword</name>
    <value>keystore keypassword</value>
</property>
<property>
    <name>ssl.client.keystore.type</name>
    <value>jks</value>
</property>
```

### Trouble Shooting

- If each machine's time is not synchronized visibly, the following error may occur. You need sync the time.

```
javax.security.sasl.SaslException: GSS initiate failed [Caused by GSSException: No valid credentials provided (Mechanism level:
Server not found in Kerberos database (7) - LOOKING_UP_SERVER)]
        at com.sun.security.sasl.gsskerb.GssKrb5Client.evaluateChallenge(GssKrb5Client.java:211)
```
