#!/bin/bash

BIN_HOME=$(cd `dirname $0`;pwd)
SSM_HOME=${BIN_HOME%/*}
SSM_NAME=${SSM_HOME##*/}
INPUT_PATH=""

echo "Do you want to install SSM to the hosts configured in the configuration file named 'agents'?"

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

cd ..;cd ..; tar cf "${SSM_NAME}.tar" ${SSM_NAME}

IFS=$'\n'
for line in `cat $SSM_NAME/conf/agents`
do
   line=$(echo $line | tr -d  " ")
   if [[ "$line" =~ ^#.* ]];then
        continue
   else
      flag=`ssh $line "if [ -d $INPUT_PATH ];then echo 1; else echo 0; fi"`
      if [ $flag = 1 ];then
         echo installing SSM to $line...
         scp ${SSM_NAME}.tar $line:$INPUT_PATH >> /dev/null
         ssh $line "cd ${INPUT_PATH};tar xf ${SSM_NAME}.tar;rm -f ${SSM_NAME}.tar"
      else
         echo "the path you type do not exist in $line"
         rm -f ${SSM_NAME}.tar
         exit 1
      fi
   fi
done

rm -f ${SSM_NAME}.tar

echo finish installing!