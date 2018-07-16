#!/bin/bash

BIN_HOME=$(cd `dirname $0`;pwd)
AGENTS_PATH=$BIN_HOME/../conf/agents
SSM_VERSION=$(cat $BIN_HOME/../../../../pom.xml | grep "<version>" | head -n 1 | tr -d "<version>/ ")
PATH1=""

echo "Do you want to install SSM to the hosts configured in the configuration file named 'agents'?"

while true;do
read -p  "Please type [Y|y] or [N|n]:
"  yn
case $yn in
        [Yy]* )
        read -p "Please type in the path where you want to install SSM:
" PATH1
        break;;
        [Nn]* ) exit 1;;
        * ) continue;;
esac
done

echo installing...

if [[ $PATH1 != */ ]];then
    PATH1=${PATH1}/
fi

cd ..;cd ..; tar cf "smart-data-${SSM_VERSION}.tar" smart-data-${SSM_VERSION}

IFS=$'\n'
for line in `cat ${AGENTS_PATH}`
do
   one=$(echo $line | tr -d  " ")
   if [[ "$one" =~ ^#.* ]];then
        continue
   else
      flag=`ssh $one "if [ -d $PATH1 ];then echo 1; else echo 0; fi"`
      if [ $flag = 1 ];then
         echo installing SSM to $one...
         scp smart-data-${SSM_VERSION}.tar $one:$PATH1 >> /dev/null
         ssh $one "cd ${PATH1};tar xf smart-data-${SSM_VERSION}.tar;rm -f smart-data-${SSM_VERSION}.tar"
      else
         echo "the path you tpye do not exist in $one"
         rm -f smart-data-${SSM_VERSION}.tar
         exit 1
      fi
   fi
done

rm -f smart-data-${SSM_VERSION}.tar

echo finish installing!
