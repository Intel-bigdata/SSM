# Integration Test 


## Pre-requests
### Python Environment
Python 2 (2.6 or higher) with `requests` installed.
```
python --version
pip install requests
```

### HDFS and SSM Environment
Make sure SSM and HDFS are correctly installed. Before executing test scripts, please set SSM's IP address in `util.py`.
```
BASE_URL = "http://{SSM_Server}:7045"
```

`{SSM_Server}` is the IP address of SSM.

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
Run all mover test cases with the following command:
```
python test_mover_10MB.py -v
python test_mover_64MB.py -v
python test_mover_1GB.py -v
python test_mover_2GB.py -v
```

### 2. Test Rule
Run all rule test cases with the following command:
```
python test_rule.py -v
```

### 3. Test Data Protection
Run all data protection test cases (read, delete, append and overwrite files during moving) with the following command:
```
python test_data_protection_1GB.py -v
python test_data_protection_2GB.py -v
```

#### Corner Cases of Data Protection

1. Test File Statue when move fails
Set a very small SSD/ARCHIVE storage on datanode. Then, move a large file to it. Then, you can see only a few or none of blocks are moved to this storage. Check if file are still readable with read action.

### 4. Test Stress/Performance
Run all stress/performance test cases with the following command:
```
python test_stress.py -v
```

If you want to increase the number of files in `hdfs:/ssmtest/`, please remove all delete file actions in `test_stress.py`.
```
for i in range(max_number):
    cids.append(delete_file(file_paths[i]))
```

## Test Scripts for smallfile

Here are several integrated tests python scripts with the instrcution of [Small File Optimization Test Plan](https://github.com/Intel-bigdata/SSM/blob/trunk/supports/small-file-test/SSM%20Small%20File%20Optimization%20Test%20Plan.md).

### Usage

#### Python Environment
All scripts are written for `python3`.

Python 3 with `requests` installed required.

#### HDFS and SSM Environment
Make sure SSM and HDFS are correctly installed. Before executing test scripts, please set SSM's IP address in `util.py`.
```
BASE_URL = "http://{SSM_Server}:7045"
```

`{SSM_Server}` is the IP address of SSM.

[`HiBench`](https://github.com/intel-hadoop/HiBench) is also required by `test_transparent_read.py`.


#### `test_ssm_smallfile_compact_rule.py`
This script is used to generate test data set with the help of TestDFSIO.
After test data set created, an SSM rule will be automatically generated and added.
Make sure it could connect to SSM before run the script.

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

Modify `workloads` variable to change tests.

Please check `-h` for further instructions.


#### HTTP Requests for SSM

It should be noticed that when connect to SSM server, make sure there is no proxy for the machine running the scripts. The following scripts require this action:
- `test_ssm_smallfile_compact_rule.py`.


### Scripts

- `test_ssm_smallfile_compact_rule.py` will generate test data set and generate SSM compact rule. Check `-h` for usage.
- `test_transparent_read.py` will test transparent read feature of SSM with `HiBench`.

