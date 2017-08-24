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
python test_mover.py -v
```

### Test Rule
Run all rule test cases with the following command:
```
python test_rule.py -v
```

### Test Data Protection
Run all data protection test cases with the following command:
```
python test_mover_protection.py -v
```

**Corner Cases:**
1. Test `Append` File during moving
Note that `append` cannot be coverd by test script. Please use HDFS `append` command during moving.
```
hdfs dfs -appendToFile data_64MB {file_path}
```
Then, read file again with read action. Check if move/read fail.

2. Test File Statue when move fails
Set a very small SSD/ARCHIVE storage on datanode. Then, move a large file to it. Then, you can see only a few or none of blocks are moved to this storage. Check if file are still readable with read action.

### Test Stress/Performance
Run all stress/performance test cases with the following command:
```
python test_stress.py -v
```

If you want to increase the number of files in `hdfs:/test/`, please remove all delete file actions in test cases.
```
for i in range(max_number):
    cids.append(delete_file(file_paths[i]))
```

