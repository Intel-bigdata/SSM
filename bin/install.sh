#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

BIN_HOME=$(cd `dirname $0`;pwd)
SSM_HOME=${BIN_HOME%/*}
SSM_NAME=${SSM_HOME##*/}
CONF_DIR=$SSM_HOME/conf
INSTALL_PATH=''
HOSTNAME=`hostname`

cd $SSM_HOME/../;

#config option is used to specify SSM's conf directory, e.g. "./install.sh --config ". If not given, the default conf is $SSM_HOME/config.
while [ $# != 0 ]; do
  case "$1" in
    "--config")
      shift
      CONF_DIR="$1"
      if [[ ! -d "${CONF_DIR}" ]]; then
        echo "ERROR : ${CONF_DIR} is not a directory"
        exit 1
      else
        if [[ $CONF_DIR != */ ]];then
             CONF_DIR=${CONF_DIR}/
        fi
        break
      fi
      ;;
      *)
      echo no command \"./install.sh $1\"
      exit 1
      ;;
   esac
done

cat $CONF_DIR/servers $CONF_DIR/agents >>/dev/null 2>/dev/null
if [ $? = 1 ];then
 echo -e "there is no file named 'agents' or 'servers' under $CONF_DIR"
 exit 1
fi

echo -e "SSM will be installed on the below hosts \033[33m(empty means there is no host configured)\033[0m"

IFS=$'\n'
for host in `cat $CONF_DIR/servers;echo '';cat $CONF_DIR/agents`
do
   host=$(echo $host | tr -d  " ")
   if [[ "$host" =~ ^#.* ]];then
        continue
   else
        echo -n -e "\033[33m$host \033[0m "
   fi
done

echo ""

user=`whoami`
if [[ "$user" = "root" ]];then
      DEFAULT_PATH=/root/
      else DEFAULT_PATH=/home/$user/
fi

while true;do
read -p  "Do you want to continue installing? Please type [Y|y] or [N|n]:
"  yn
case $yn in
        [Yy]* )
        read -p "$(echo -e "Please type in the path where you want to install SSM \033[33m(empty means using default path '$DEFAULT_PATH')\033[0m":)
" INSTALL_PATH
        break;;
        [Nn]* ) exit 1;;
        * ) continue;;
esac
done

if [ -z "$INSTALL_PATH"  ];then
     INSTALL_PATH=$DEFAULT_PATH
fi

echo installing...

if [[ $INSTALL_PATH != */ ]];then
    INSTALL_PATH=$(echo $INSTALL_PATH | tr -d  " ")
    INSTALL_PATH=${INSTALL_PATH}/
fi

TARGET=""
FLAG=0
ipOrHost=0
for TARGET in `cat $CONF_DIR/servers;echo '';cat $CONF_DIR/agents`
do
   TARGET=$(echo $TARGET | tr -d  " ")
   for ipOrHost in `echo $(hostname -A;hostname -I) | sed 's/ /\n/g';echo "localhost";echo "$HOSTNAME";echo "127.0.1.1";echo "127.0.0.1"`
   do
        ipOrHost=$(echo $ipOrHost | tr -d  " ")
        if [[ "$TARGET" == "$ipOrHost" ]];then
            FLAG=1
            break
        fi
   done
   if [[ $FLAG == "1" ]];then
        break
   fi
done

tar cf "${SSM_NAME}.tar" ${SSM_NAME}

ARRAY=()
fqwc=0


for host in `cat $CONF_DIR/servers;echo '';cat $CONF_DIR/agents`
do
   host=$(echo $host | tr -d  " ")
   if [[ "$host" =~ ^#.* ]];then
        continue
   else
      
      for element in ${ARRAY[@]}
      do
         if [ "$host" == "$element" ];then
 	    fqwc=1    
	    break
         fi
      done

      if [ $fqwc -eq 1 ];then
	 fqwc=0         
         continue
      else
         ARRAY+=("$host")
      fi

      #Before install on a host, delete ssm home directory if there exists
      if [[ "$host" != "$TARGET" ]];then
         ssh $host "if [ -d ${INSTALL_PATH}${SSM_NAME} ];then rm -rf ${INSTALL_PATH}${SSM_NAME};fi"
      else
         SSM_UP_HOME=${SSM_HOME%/*}/
         if [[ "$SSM_UP_HOME" != "${INSTALL_PATH}" ]];then
            if [ -d ${INSTALL_PATH}${SSM_NAME} ];then rm -rf ${INSTALL_PATH}${SSM_NAME};fi
         else
            continue
         fi
      fi
      flag=`ssh $host "if [ -d $INSTALL_PATH ];then echo 1; else echo 0; fi"`
      if [ $flag = 1 ];then
         echo installing SSM to $host...
         scp ${SSM_NAME}.tar $host:$INSTALL_PATH >> /dev/null
         ssh $host "cd ${INSTALL_PATH};tar xf ${SSM_NAME}.tar;rm -f ${SSM_NAME}.tar"
      elif [ $flag = 0 ];then
         ssh $host "mkdir $INSTALL_PATH"
         echo installing SSM to $host...
         scp ${SSM_NAME}.tar $host:$INSTALL_PATH >> /dev/null
         ssh $host "cd ${INSTALL_PATH};tar xf ${SSM_NAME}.tar;rm -f ${SSM_NAME}.tar"
      else
         rm -f ${SSM_NAME}.tar
         exit 1
      fi
   fi
done

rm -f ${SSM_NAME}.tar
