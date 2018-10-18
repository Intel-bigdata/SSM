# SSM Erasure Coding (EC) on HDFS 3.x

### 1. SSM EC action

#### ec -file $path \[-policy $policy -bufSize $bufSize\]

#### (1) Directory case

If $path is a dir, set its EC policy to the given one, which has no impact on the existed files under it but can make new files stored in EC way henceforward.

#### (2) File case

If $path is a file, read the file content with append permission (for locking file, may then need read permission for read) and write into a temp file in EC way.
The temp file is named by fileName, aidxxx and timestamp in millisecond with "_" separated /system/ssm/ec_tmp. The file attributes (owner, permission, atime, mtime, etc.) will be kept consistent.
After the EC temp file is created, it will be renamed to original file. So the original file will be overwritten.

#### (3) EC policy

If no value is set for option -policy, the default EC policy will be used. User can configure default policy in HDFS conf. Replication is a special EC policy in HDFS.
So if `replication` is passed to an EC action, SSM will convert the given file to the one stored in replication way.
Moreover, if the submitted EC action is given a same EC policy as this file’s current one, SSM has no need to and will not execute the conversion operation.

#### (4) Buffer size

If no value is set for option -bufSize, SSM will use the default value in byte equivalent to 1MB. SSM supports human-readable value for bufSize, e.g., 1KB, 1MB.

#### (5) Example

`file: path matches "/foo/*" and accessCount(30day) < 3 | ec -policy RS-3-2-1024k`

For each file in /foo dir, if it is accessed less than 3 times in 30 days, convert it to the one stored in EC way with the EC policy named RS-3-2-1024k.

### 2. SSM unEC action

#### unec -file $path \[-bufSize $bufSize\]

#### (1) Directory case

If $path is a dir, just set the policy to `replication`.

#### (2) File case

If $path is a file, SSM will convert the file to the one stored in replication way.

#### (3) Buffer size

If no value is set for option -bufSize, SSM will use the default value in byte equivalent to 1MB. SSM supports human-readable value for bufSize, e.g., 1KB, 1MB.

### 3. A series of SSM actions about EC policy

#### (1) listec

List the information of all EC policies supported in HDFS.

#### (2) checkec -file $path

Show the EC policy information for $path. The given $path can either be a file or a directory.

#### (3) addec -codec $codecName -dataNum $dataNum -parityNum $parityNum -cellSize $cellSize

Add an EC policy with the given parameters. The cellSize in byte should be divisible by 1024. SSM supports human-readable value for cellSize, e.g., 1KB, 1MB.

#### (4) enableec	-policy $policyName

Enable the given EC policy named $policyName.

#### (5) disableec -policy $policyName

Disable the given EC policy named $policyName.

#### (6) removeec	-policy $policyName

Remove the given EC policy named $policyName.


### 4. Track EC state for each file in HDFS

SSM can track whether a file is stored in EC way and persist such info into SSM MetaStore. So if user submits a rule with EC involved, SSM can just check a file’s EC state from MetaStore and trigger corresponding action with low latency.

### 5. Throttle for SSM EC

EC can consume large resources, especially network IO. SSM has a throttle to avoid cluster overload. When a threshold is reached, SSM will delay dispatching more EC action if any.
By default, the throttle is turned off. To make the throttle work, user needs to set a positive value for the property named `smart.action.ec.throttle.mb` in smart-default.xml.

### 6. Note
You may see the log similar to the following from SSM when you run enormous EC tasks. Because by default HDFS enables a strategy that considers each datanode's load when writing data. If some nodes are under high loads, they will be excluded temporarily.
HDFS will recover the corresponding blocks later. To disable this strategy, you need set the value as `false` for a property named `dfs.namenode.replication.considerLoad` in hdfs-site.xml.
```
2018-10-18 14:23:31,212 WARN org.apache.hadoop.hdfs.DFSOutputStream.logCorruptBlocks 1296: Block group <1> failed to write 1 blocks.
```