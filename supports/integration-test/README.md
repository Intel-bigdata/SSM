# Integration Test 


## Pre-requests
### Python Environment
Python 2.6 or higher, `requests`.
```
python --version
pip install requests
```

### HDFS and SSM Environment
Make sure SSM and HDFS are correctly installed. Before executing test script, please make sure SSM address is correctly configured in `util.py`.
```
BASE_URL = "http://{SSM_Server}:7045"
```

Here `{SSM_Server}` is the IP address of SSM.

## Init/Rest Test Environment
1. Remove all files in hdfs:/test/
Run this command in HDFS enviroment.
```
HDFS dfs -ls -rm -r /test/
HDFS dfs -mkdir /test/
```

2. Create 10000 files (1MB) in hdfs:/test/

```
python reset_env.py -v
```

3. Init/Rest SSM
```
bin/stop-ssm.sh
bin/init-ssm.sh
bin/start-ssm.sh
```

## Run Test Scripts
### Test Mover
Run all mover test cases with the following command:
```
python test_mover_10MB.py -v
python test_mover_64MB.py -v
python test_mover_1GB.py -v
python test_mover_2GB.py -v
```

### Test Rule
Run all rule test cases with the following command:
```
python test_rule.py -v
```

### Test Data Protection
Run all data protection test cases with the following command:
```
python test_data_protection_1GB.py -v
python test_data_protection_2GB.py -v
```

### Corner Cases

1. Data Protection--Test `append` File during moving
Note that `append` cannot be coverd by test scripts. Please use HDFS `appendToFile` command during moving. 

For example, you can create a large file (3GB) and a small file (64MB).
```
fallocate -l 3G data_3GB
fallocate -l 64M data_64MB
hdfs dfs -put data_3GB /test/data_3GB
```
Then, submit a allssd action (with UI/Restfull API)to move large file to allssd. During moving you can append a small file to it with `appendToFile` command.
```
hdfs dfs -appendToFile data_64MB /test/data_3GB
```

2. Data Protection--Test File Statue when move fails
Set a very small SSD/ARCHIVE storage on datanode. Then, move a large file to it. Then, you can see only a few or none of blocks are moved to this storage. Check if file are still readable with read action.

### Test Stress/Performance
Run all stress/performance test cases with the following command:
```
python test_stress.py -v
```

If you want to increase the number of files in `hdfs:/test/`, please remove all delete file actions in `test_stress.py`.
```
for i in range(max_number):
    cids.append(delete_file(file_paths[i]))
```

