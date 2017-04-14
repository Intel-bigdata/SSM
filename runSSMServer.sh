#!/bin/sh
SSMSERVER_HOME=hadoop-ssm-project-3.0.0-alpha2-SNAPSHOT-cache.jar
SSMPOC_HOME=/home/workspace/shunyang/ssmpoc/hadoop
LIB_DIR1=lib
LIB_DIR2=/usr/share/scala/lib
CLASS_PATH=$SSMPOC_HOME/hadoop-hdfs-project/hadoop-hdfs/target/hadoop-hdfs-3.0.0-alpha2-SNAPSHOT.jar:$SSMPOC_HOME/hadoop-common-project/hadoop-common/target/hadoop-common-3.0.0-alpha2-SNAPSHOT.jar:$SSMPOC_HOME/hadoop-hdfs-project/hadoop-hdfs-client/target/hadoop-hdfs-client-3.0.0-alpha2-SNAPSHOT.jar:$(echo $LIB_DIR1/*.jar | tr ' ' ':'):$(echo $LIB_DIR2/*.jar | tr ' ' ':'):
java -Xbootclasspath/a:"$CLASS_PATH" -cp $SSMSERVER_HOME org.apache.hadoop.ssm.SSMServer

