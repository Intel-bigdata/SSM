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
INPUT_PATH=""
FLAG=false

echo "Do you want to install SSM to the hosts configured in the files 'agents' and 'servers'?"

while true;do
read -p  "Please type [Y|y] or [N|n]:
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

cd $SSM_HOME/../; tar cf "${SSM_NAME}.tar" ${SSM_NAME}

IFS=$'\n'
for host in `cat $SSM_NAME/conf/servers;echo '';cat $SSM_NAME/conf/agents`
do
   host=$(echo $host | tr -d  " ")
   if [[ "$host" =~ ^#.* ]];then
        continue
   else
      flag=`ssh $host "if [ -d $INPUT_PATH ];then echo 1; else echo 0; fi"`
      if [ $flag = 1 ];then
         FLAG=true
         echo installing SSM to $host...
         scp ${SSM_NAME}.tar $host:$INPUT_PATH >> /dev/null
         ssh $host "cd ${INPUT_PATH};tar xf ${SSM_NAME}.tar;rm -f ${SSM_NAME}.tar"
         echo install SSM to $host successfully!
      elif [ $flag = 0 ];then
         FLAG=true
         ssh $host "mkdir $INPUT_PATH"
         if [ $? = 0 ];then
            echo installing SSM to $host...
            scp ${SSM_NAME}.tar $host:$INPUT_PATH >> /dev/null
            ssh $host "cd ${INPUT_PATH};tar xf ${SSM_NAME}.tar;rm -f ${SSM_NAME}.tar"
            echo install SSM to $host successfully!
         else
            exit 1
         fi
      elsegit
         rm -f ${SSM_NAME}.tar
         exit 1
      fi
   fi
done

rm -f ${SSM_NAME}.tar

if [ $FLAG = false ];then
    echo -e No hosts configured in \'servers\' and \'agents\'!
fi