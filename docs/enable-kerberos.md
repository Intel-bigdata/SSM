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

### Config krb.conf
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

### Init database
```
kdb5_util create -r HADOOP.COM -s
```

### Start Kerberos
```
/usr/local/sbin/krb5kdc
/usr/local/sbin/kadmind
```

## 2. Export the keytabs

### Add ssmserver principal and export it to keytab
```
kadmin.local:addprinc -randkey ssmserver/hostname
kadmin.local:xst -k /xxx/xxx/ssmserver-hostname.keytab ssmserver/hostname
```

### Add ssmagent principals and export it to keytabs
```
kadmin.local:addprinc -randkey ssmagent/hostname
kadmin.local:xst -k /xxx/xxx/ssmagent-hostname.keytab ssmagent/hostname
```

## 3. Update smart-site.xml
```
    <property>
        <name>smart.security.enable</name>
        <value>true</value>
    </property>
    <property>
        <name>smart.server.keytab.file</name>
        <value>/xxx/xxx/ssmserver-hostname.keytab</value>
    </property>
    <property>
        <name>smart.server.kerberos.principal</name>
        <value>ssmserver/_HOST@HADOOP.COM</value>
    </property>
    <property>
        <name>smart.agent.keytab.file</name>
        <value>/xxx/xxx/ssmagent-hostname.keytab</value>
    </property>
    <property>
        <name>smart.agent.kerberos.principal</name>
        <value>ssmagent/_HOST@HADOOP.COM</value>
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
