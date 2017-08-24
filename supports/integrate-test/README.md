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
1. Init/Rest SSM
```
bin/init-ssm.sh
```

2. Remove all files in hdfs:/test/
Run this command in HDFS enviroment.
```
HDFS dfs -ls -rm -r /test/
```

3. Create 10000 files (1MB) in hdfs:/test/

```
python reset_env.py -v
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

### Test Stress/Performance
Run all stress/performance test cases with the following command:
```
python test_stress.py -v
```



