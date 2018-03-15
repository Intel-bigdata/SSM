ssh sr518 "hdfs dfs -rm -r /10KB_10000"
ssh sr518 "hdfs dfs -mkdir /10KB_10000"
ssh sr518 "hdfs dfs -rm -r /1MB_10000"
ssh sr518 "hdfs dfs -mkdir /1MB_10000"
ssh sr518 "hdfs dfs -rm -r /100MB_1000"
ssh sr518 "hdfs dfs -mkdir /100MB_1000"
ssh sr518 "drop-cache"
ssh sr519 "drop-cache"
drop-cache