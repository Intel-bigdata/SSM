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

## Init/Reset Test Environment
1. Remove all files in hdfs:/ssmtest/
Run this command in HDFS environment.
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
The value of size is 64MB if not given.

### 2. Test Rule
```
python test_rule.py -v
```
This command will run all test cases for rule.

### 3. Test Data Protection
```
python test_data_protection.py -size 2GB -v
```
This command will run all test cases (read, delete, append and overwrite files during moving) for 10MB files.
The value of size is 1GB if not given. A large value is recommended.

#### Corner Cases of Data Protection

1. Test file status when move fails
Set a very small SSD/ARCHIVE storage on datanode. Then, move a large file to it. Then, you can see only a few or none of blocks are moved to this storage. Check if this file is still readable with read action.

### 4. Test Copy Data to S3

```
python test_s3.py -size 10MB -num 10 -v
```
This command will run copy to S3 test on 10 files each of which is 10MB.
The values of size and num are 1MB and 100 respectively if not given.

### 5. Test Stress/Performance

#### Cmdlet Stress
```
python test_stress_cmdlet.py -size 10MB -num 1000 -v
```
This command will run test for create, read, delete action on 1000 files each of which is 10MB.
The values of size and num are 1MB and 10000 respectively if not given.

#### Mover Stress
```
python test_stress_mover.py -size 10MB -num 1000 -v
```
This command will run mover test on 1000 files each of which is 10MB.
The values of size and num are 1MB and 10000 respectively if not given.

#### Rule Stress
```
python test_stress_rule.py -num 10 -v
```
This command will trigger 10 rules on files under TEST_DIR.
The value of num is 100 if not given.

#### Sync Stress
```
python test_stress_sync.py -size 10MB -num 1000 -dest hdfs://sr518:9000/dest/ -v
```
This command will trigger a rule which will sync 1000 10MB files to hdfs://sr518:9000/dest.
The values of size and num are 1MB and 10000 respectively if not given.
For dest, its default value is "/dest/" which means a directory of SSM's HDFS cluster.

### 6. Test Scripts for Small File Optimization

This is a guide for SSM small file integration test with python scripts under the instruction of [Test Plan](https://github.com/Intel-bigdata/SSM/blob/trunk/supports/small-file-test/SSM%20Small%20File%20Optimization%20Test%20Plan.md).
Note that user can get further instructions of below python scripts via parameter `-h`.

#### Small File Rule
```
python test_small_file_rule -n 10 -s 10KB
```
This command will generate 10 files under TEST_DIR, the size of every file is 10KB.         
Then trigger a small file compact rule on these files.

#### Small file compact action
```
python test_small_file_actions -a compact -n 5 -s 10KB
```
This command will generate 5 files under TEST_DIR, the size of every file is 10KB.          
Then trigger a small file compact action on these files, the default container file of these files is /container_tmp_file.

#### Small file uncompact action
```
python test_small_file_actions -a uncompact -c /_container_tmp_file
```
This command will trigger a small file uncompact action on the container file: /container_tmp_file.

#### Smart File System
> [`HiBench`](https://github.com/intel-hadoop/HiBench) is required for this test case.

```
python test_smart_file_system.py -d /root/HiBench
```
This command will run workloads of HiBench, the test data are compacted by automatically generated small file compact rule.

The default workloads are following:
- 'micro/wordcount',
- 'micro/sort',
- 'micro/terasort',
- 'ml/bayes',
- 'sql/scan',
- 'sql/join',
- 'sql/aggregation',
- 'websearch/pagerank'      

User can specify workloads by altering `workloads` variable in `test_smart_file_system.py`.

### 7. Test EC/unEC

#### Test EC/unEC action
To test a given EC policy, it should be enabled in HDFS beforehand. You can also trigger an enableec action in SSM to enable an EC policy.
```
python test_ec_action.py -size 10MB -policy XOR-2-1-1024k -v
```
This command will test EC & UnEC actions on a 10MB file. In the test, the EC action will use an EC policy named XOR-2-1-1024k.

#### Test rule with EC/unEC action
```
python test_ec_rule.py -size 10MB -num 10 -policy XOR-2-1-1024k -v
```
This command will test rules with EC/unEC actions respectively on 10 10MB files. In the test, the EC action will use an EC policy named XOR-2-1-1024k.

## Tips

#### HTTP Requests for SSM

It should be noticed that http_proxy can make SSM's web service IP or hostname unrecognized. So you may have to unset http_proxy or set no_proxy on the host where you run the test.

