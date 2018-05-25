# Integration Tests for SSM

Here are several integrated tests python scripts with the instrcution of [Small File Optimization Test Plan](https://github.com/Intel-bigdata/SSM/blob/smallfile/supports/small-file-test/SSM%20Small%20File%20Optimization%20Test%20Plan.md).

### Usage

All scripts are written for `python3`.

##### `test_ssm_compact.py`
This script is used to generate test data set with the help of TestDFSIO.
After test data set created, an SSM rule will be automatically generated and added.
Make sure it could connect to SSM before run the script.

##### `test_transparent_read.py`
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
- `test_ssm_compact.py`.


### Scripts

- `test_ssm_compact.py` will generate test data set and generate SSM compact rule. Check `-h` for usage.
- `test_transparent_read.py` will test transparent read feature of SSM with `HiBench`.
