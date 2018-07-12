#/bin/bash

BIN_HOME=$(cd `dirname $0`;pwd)
AGENTS_PATH=${BIN_HOME%/*}/conf/agents
PATH=""

trim()
{
     var=$1
    var=${var%% }
    var=${var## }
    echo  $var
}

askForPath()
{
   read -p "Please type in the path where you want to install SSM:" PATH
}

echo "Do you want to install SSM to the hosts configured in the configuration file agents?"

while true;do
read -p  "Please type yes or no:
"  yn
case $yn in
        [Yy]* ) askForPath;break;;
        [Nn]* ) exit 1;;
        * ) continue;;
esac
done

echo installing...


while read line
do
    if [[ $line != "" ]];then
       line=${line## }
       line=${line%% }
       scp $line
    fi

done < $AGENTS_PATH






