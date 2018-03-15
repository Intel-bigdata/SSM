for m in 30 60 90
do
for name in 10KB_10000 1MB_10000 100MB_1000
do
for i in {1..5}
do
    ssh sr518 "hdfs dfs -rm -r /10KB_10000"
    ssh sr518 "hdfs dfs -rm -r /1MB_10000"
    ssh sr518 "hdfs dfs -rm -r /100MB_1000"
    ssh sr518 "drop-cache"
    drop-cache
    echo "====================m:$m  file:$name  time:$i============================"
    echo "hadoop distcp -m $m \\
          hdfs://sr519:9000/$name \\
          hdfs://sr518:9000/$name \\
          > /root/sorttest/PAT/PAT-collecting-data/results/$name-$i-$m.log 2>&1" > cmd.sh
    ./pat run "$name-$i-$m"
done
done
done