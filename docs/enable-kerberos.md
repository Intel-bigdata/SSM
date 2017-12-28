## 1. Deploy Kerberos KDC

### Install Kerberos
```
yum install -y krb5-server krb5-lib krb5-workstation
```

### Config kdc.conf
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


### Config krb5.conf
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

### Create the KDC database
The following is an example of how to create a Kerberos database and stash file on the master KDC, using the kdb5_util command. Replace HADOOP.COM with the name of your Kerberos realm.
```
kdb5_util create -r HADOOP.COM -s
```
This will create five files in LOCALSTATEDIR/krb5kdc (or at the locations specified in kdc.conf):

### Start the Kerberos daemons on the master KDC
```
shell% krb5kdc
shell% kadmind
```

## 2. Export the keytabs

### Add smartserver Kerberos principal to database and export it to keytab. Replace hostname with the hostname of smart server.
```
kadmin.local:addprinc -randkey smartserver /hostname
kadmin.local:xst -k /xxx/xxx/smartserver-hostname.keytab smartserver /hostname
```

### Add smartagent Kerberos principals to database and export it to keytabs. Replace hostname with the hostname of smart agent, please create principals for each agents.
```
kadmin.local:addprinc -randkey smartagent/hostname
kadmin.local:xst -k /xxx/xxx/smartagent-hostname.keytab smartagent/hostname
```

### Add hdfs Kerberos principals to database and export it to keytabs.
```
kadmin.local:addprinc -randkey hdfs/hostname
kadmin.local:xst -k /xxx/xxx/hdfs-hostname.keytab hdfs/hostname
```

## 3. Update smart-site.xml
```
<property>
  <name>smart.security.enable</name>
  <value>true</value>
</property>
<property>
  <name>smart.server.keytab.file</name>
  <value>/xxx/xxx/smartserver-hostname.keytab</value>
</property>
<property>
  <name>smart.server.kerberos.principal</name>
  <value>smartserver/_HOST@HADOOP.COM</value>
</property>
<property>
  <name>smart.agent.keytab.file</name>
  <value>/xxx/xxx/smartagent-hostname.keytab</value>
</property>
<property>
  <name>smart.agent.kerberos.principal</name>
  <value>smartagent/_HOST@HADOOP.COM</value>
</property>
```

## 4. Update hadoop configuration files
 
### Update core-site.xml
add the following properties:
```
<property>
  <name>hadoop.security.authorization</name>
  <value>true</value>
</property>
<property>
  <name>hadoop.security.authentication</name>
  <value>kerberos</value>
</property>
<property>
   <name>hadoop.security.authentication.use.has</name>
   <value>true</value>
</property>
```
### Update hdfs-site.xml
add the following properties:
```
<!-- General HDFS security config -->
<property>
  <name>dfs.block.access.token.enable</name>
  <value>true</value>
</property>

<!-- NameNode security config -->
<property>
  <name>dfs.namenode.keytab.file</name>
  <value>/xxx/xxx/hdfs-hostname.keytab</value>
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

<!-- DataNode security config -->
<property>
  <name>dfs.datanode.data.dir.perm</name>
  <value>700</value>
</property>
<property>
  <name>dfs.datanode.keytab.file</name>
  <value>/xxx/xxx/hdfs-hostname.keytab</value>
</property>
<property>
  <name>dfs.datanode.kerberos.principal</name>
  <value>hdfs/_HOST@HADOOP.COM</value>
</property>


## 5. Deploy HTTPS
===============

### 1. Create a keystore file for each host

> keystore: the keystore file that stores the certificate.      
> validity: the valid time of the certificate in days.
```
keytool -alias {hostname} -keystore {keystore} -validity {validity} -genkey
```

> The keytool will ask for more details such as the keystore password, keypassword and CN(hostname).

### 2. Export the certificate public key to a certificate file for each host
```
keytool -export -alias {hostname} -keystore {keystore} -rfc -file {cert-file}
```

### 3. Create a common truststore file (trustAll)
The truststore file contains the public key from all certificates. If you assume a 2-node cluster with node1 and node2,
login to node1 and import the truststore file for node1.
```
keytool -import -alias {hostname} -keystore {trustAll} -file {cert-file}
```

### 4. Update the common truststore file
* Move {trustAll} from node1 to node2 ({trustAll} already has the certificate entry of node1), and repeat Step 3.

* Move the updated {trustAll} from node2 to node1. Repeat these steps for each node in the cluster.
When you finish, the {trustAll} file will have the certificates from all nodes.

> Note these work could be done on the same node, just notice the hostname.

### 5. Copy {trustAll} from node1 to all of the other nodes

### 6. Validate the common truststore file
```
keytool -list -v -keystore {trustAll}
```

### 7. Edit the Configuration files
> Deploy {keystore} and {trustAll} files and config /etc/has/ssl-server.conf for HAS server
```
ssl.server.keystore.location = {path to keystore}
ssl.server.keystore.password = {keystore password set in step 1}
ssl.server.keystore.keypassword = {keypassword set in step 1}
ssl.server.truststore.reload.interval = 1000
ssl.server.truststore.location = {path to trustAll}
ssl.server.truststore.password = {trustAll password set in step 2}
```

> Config /etc/has/ssl-client.conf for HAS client
```
ssl.client.truststore.location = {path to trustAll}
ssl.client.truststore.password = {trustAll password}
```

> Config $HADOOP_HOME/etc/hadoop/ssl-server.xml for Hadoop
```
<configuration>

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

</configuration>
```

> Config $HADOOP_HOME/etc/hadoop/ssl-client.xml for Hadoop
```
<configuration>

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

</configuration>
```

