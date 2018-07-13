#!/bin/bash

BIN_HOME=$(cd `dirname $0`;pwd)
SSM_HOME=${BIN_HOME%/*}
PROJ_HOME=${SSM_HOME%/*}
SSM_NAME=${SSM_HOME##*/}
AGENTS_PATH=$SSM_HOME/conf/agents
PATH=""

echo "Do you want to install SSM to the hosts configured in the configuration file named 'agents'?"

while true;do
read -p  "Please type yes or no:
"  yn
case $yn in
        [Yy]* )
        read -p "Please type in the path where you want to install SSM:
" PATH
        break;;
        [Nn]* ) exit 1;;
        * ) continue;;
esac
done

echo installing...

if [[ $PATH != */ ]];then
    PATH=${PATH}/
fi

tar cvf $PROJ_HOME/${SSM_NAME}.tar $SSM_HOME

for line in `cat $AGENTS_PATH`
do
       flag=`ssh $line "if [ -d $PATH ];then echo 1; else echo 0; fi"`
       if [ $flag = 1 ];then
           echo installing SSM to ${line}...
           scp $PROJ_HOME/${SSM_NAME}.tar $line:$PATH
           ssh $line "tar xvf ${PATH}${SSM_NAME}.tar"
           ssh $line "rm -f  ${PATH}${SSM_NAME}.tar"
       else
           echo "the path you tpye don not exist in $line"
           rm -f $PROJ_HOME/${SSM_NAME}.tar
           exit 1
       fi
done

rm -f $PROJ_HOME/${SSM_NAME}.tar

echo finish installing!



