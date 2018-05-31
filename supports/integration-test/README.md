# Integration Test 


## Pre-requests
### Python Environment
Python 2 (2.6 or higher) with `requests` and `timeout-decorator` installed.
```
python --version
pip install requests
pip install timeout-decorator
```

### HDFS and SSM Environment
Make sure SSM and HDFS are correctly installed. Before executing test scripts, please set SSM's web UI address in `util.py`.
```
BASE_URL = "http://{SSM_Server}:7045"
```

`{SSM_Server}` is the IP address or hostname of active Smart Server.

## Init/Rest Test Environment
1. Remove all files in hdfs:/ssmtest/
Run this command in HDFS enviroment.
```
HDFS dfs -ls -rm -r /ssmtest/
HDFS dfs -mkdir /ssmtest/
```

2. Create 10000 files (1MB) in hdfs:/ssmtest/

```
python reset_env.py -v
```

3. Stop and start SSM with formatting database

```
bin/stop-ssm.sh
bin/start-ssm.sh -format
```

## Run Test Scripts
### 1. Test Mover
```
python test_mover.py -size 10MB -v

```
This command will run all test cases for 10MB files.
The default value of size is 64MB if not given.

### 2. Test Rule
```
python test_rule.py -v
```
This command will run all test cases for rule.

### 3. Test Data Protection
```
python test_data_protection.py -size 10MB -v
```
This command will run all test cases (read, delete, append and overwrite files during moving) for 10MB files.
The default value of size is 64MB if not given.

#### Corner Cases of Data Protection

1. Test file status when move fails
Set a very small SSD/ARCHIVE storage on datanode. Then, move a large file to it. Then, you can see only a few or none of blocks are moved to this storage. Check if this file is still readable with read action.

### 4. Test Stress/Performance

#### Cmdlet Stress
```
python test_S3.py -size 10MB -num 10 -v
```
This command will run copy to S3 test on 10 files each of which is 10MB.
The default values of size and num are 1MB and 100 respectively if not given.

### 5. Test Stress/Performance

#### Cmdlet Stress
```
python test_stress_cmdlet.py -size 10MB -num 1000 -v
```
This command will run test for create, read, delete action on 1000 files each of which is 10MB.
The default values of size and num are 1MB and 10000 respectively if not given.

#### Mover Stress
```
python test_stress_mover.py -size 10MB -num 1000 -v
```
This command will run mover test on 1000 files each of which is 10MB.
The default values of size and num are 1MB and 10000 respectively if not given.

#### Rule Stress
```
python test_stress_rule.py -num 10 -v
```
This command will trigger 10 rules on files under TEST_DIR.
The default value of num is 100 if not given.

#### Sync Stress
```
python test_stress_sync.py -size 10MB -num 1000 -v
```
This command will run sync test on 1000 files each of which is 10MB.
The default values of size and num are 1MB and 10000 respectively if not given.

### 6. Test Scripts for Small File Optimization

This is a guide for SSM small file integration test with python scripts under the instruction of  [Test Plan](https://github.com/Intel-bigdata/SSM/blob/trunk/supports/small-file-test/SSM%20Small%20File%20Optimization%20Test%20Plan.md).

#### Dependency

[`HiBench`](https://github.com/intel-hadoop/HiBench) is also required by `test_transparent_read.py`.

#### `test_generate_test_set.py`
This script will generate the required data set.

#### `test_smallfile_action.py`
This script will generate the required data set, and then generate a new SSM compact or uncompact action.

#### `test_smallfile_compact_rule.py`
This script will generate the required data set, and then generate a new SSM compact rule.

#### `test_transparent_read.py`
This script is used to run SSM transparent read test with HiBench for SSM integration test.
The following workloads are included:
- 'micro/wordcount', 
- 'micro/sort', 
- 'micro/terasort', 
- 'ml/bayes',
- 'sql/scan', 
- 'sql/join', 
- 'sql/aggregation', 
- 'websearch/pagerank'

You can just use `workload` as the variable in python script.

Please check `-h` for further instructions.


#### Scripts

- `test_generate_test_set.py` will generate test data set. Check `-h` for usage.
- `test_smallfile_action.py` will generate test data set and submit a new action. Check `-h` for usage.
- `test_smallfile_compact_rule.py` will generate test data set and submit a new compact rule. Check `-h` for usage.
- `test_transparent_read.py` will test transparent read feature of SSM with `HiBench`. Check `-h` for usage.


## Tips

#### HTTP Requests for SSM

It should be noticed that http_proxy can make SSM's web service IP or hostname unrecognized. So you may have to unset http_proxy or set no_proxy on the host where you run the test.

