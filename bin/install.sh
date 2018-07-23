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
INPUT_PATH=""

cd $SSM_HOME/../;

#config option to let user give conf directory, "./install.sh --config" . If not given, use the default conf
while [ $# != 0 ]; do
  case "$1" in
    "--config")
      shift
      CONF_DIR_TMP="$1"
      if [[ ! -d "${CONF_DIR_TMP}" ]]; then
        echo "ERROR : ${CONF_DIR_TMP} is not a directory"
        exit 1
      else
        CONF_DIR="$1"
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
 echo -e "maybe there is no 'agents' or 'servers' under $CONF_DIR"
 exit 1
fi

echo -e "The configured hosts are shown below(\033[34mempty means there is no host configured\033[0m):"

IFS=$'\n'
for showhost in `cat $CONF_DIR/servers;echo '';cat $CONF_DIR/agents`
do
   showhost=$(echo $showhost | tr -d  " ")
   if [[ "$showhost" =~ ^#.* ]];then
        continue
   else
        echo -n -e "\033[34m$showhost \033[0m "
   fi
done

echo ""

while true;do
read -p  "Do you want to continue installing? Please type [Y|y] or [N|n]:
"  yn
case $yn in
        [Yy]* )
        read -p "Please type in the path where you want to install SSM:
" INPUT_PATH
        break;;
        [Nn]* ) exit 1;;
        * ) continue;;
esac
done

echo installing...

if [[ $INPUT_PATH != */ ]];then
    INPUT_PATH=${INPUT_PATH}/
fi

tar cf "${SSM_NAME}.tar" ${SSM_NAME}

for host in `cat $CONF_DIR/servers;echo '';cat $CONF_DIR/agents`
do
   host=$(echo $host | tr -d  " ")
   if [[ "$host" =~ ^#.* ]];then
        continue
   else
      #Before install on a host, delete ssm home directory if there exists
      if_exist=`ssh $host "if [ -d ${INPUT_PATH}${SSM_NAME} ];then echo 1; else echo 0; fi"`
      if [ $if_exist = 1 ];then
          ssh $host "rm -rf ${INPUT_PATH}${SSM_NAME}"
      fi
      flag=`ssh $host "if [ -d $INPUT_PATH ];then echo 1; else echo 0; fi"`
      if [ $flag = 1 ];then
         echo installing SSM to $host...
         scp ${SSM_NAME}.tar $host:$INPUT_PATH >> /dev/null
         ssh $host "cd ${INPUT_PATH};tar xf ${SSM_NAME}.tar;rm -f ${SSM_NAME}.tar"
         echo install SSM to $host successfully!
      elif [ $flag = 0 ];then
         ssh $host "mkdir $INPUT_PATH"
         if [ $? = 0 ];then
            echo installing SSM to $host...
            scp ${SSM_NAME}.tar $host:$INPUT_PATH >> /dev/null
            ssh $host "cd ${INPUT_PATH};tar xf ${SSM_NAME}.tar;rm -f ${SSM_NAME}.tar"
            echo install SSM to $host successfully!
         else
            exit 1
         fi
      else
         rm -f ${SSM_NAME}.tar
         exit 1
      fi
   fi
done

rm -f ${SSM_NAME}.tar