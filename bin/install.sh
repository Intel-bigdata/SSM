#!/bin/bash

BIN_HOME=$(cd `dirname $0`;pwd)
SSM_HOME=${BIN_HOME%/*}
SSM_NAME=${SSM_HOME##*/}
AGENTS_PATH=$BIN_HOME/../conf/agents
PATH1=""
echo $SSM_HOME
echo $SSM_NAME
echo $AGENTS_PATH


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

cd ..;cd ..; tar cf "${SSM_NAME}.tar" ${SSM_NAME}

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
         scp ${SSM_NAME}.tar $one:$PATH1 >> /dev/null
         ssh $one "cd ${PATH1};tar xf ${SSM_NAME}.tar;rm -f ${SSM_NAME}.tar"
      else
         echo "the path you tpye do not exist in $one"
         rm -f ${SSM_NAME}.tar
         exit 1
      fi
   fi
done

rm -f ${SSM_NAME}.tar

echo finish installing!
