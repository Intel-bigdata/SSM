Application API
===============

Application APIs are used by applications that run on top of HDFS. This set
of APIs includes cache,uncache, and enforce storage policy file level operations. The system will
execute the file operation on behalf of the application, with the privilege of
the user who started the application. SSM will provide a SmartDFSClient
which includes both HDFS DFSClient functions and new SSM Application
APIs. Upper level applicatiosn can use this SmartDFSClient instead of the
original HDFS DFSClient. Here is the diagram.

<img src="./image/api.png" width="554" height="408" />

SmartDFSClient API
------------
  
* void **cache**(**String** filePath) **throws** IOException;

  Cache a file

* void **uncache**(**String** filePath) **throws** IOException;

  Uncache a file
* void **applyStoragePolicy**(**String** filePath, **String** policyName) **throws** IOException;

  Set the storage policy on the file and enforce the same.

SmartClient API
------------

* String\[\] **getSupportedActions**() **throws** IOException;

  List all action names currently supported by the system. Current supported actions name are “enforceStoragePolicy”, “cache”, “uncache” etc.

* void **executeAction**(**String** actionName, **String\[\]** actionParams) **throws** IOException;

  A synchronized generic API to execute action. System will maintain an internal task to performance the action. The API will return until the task is finished.
  
* void **executeActionAsync**(**String** actionName, **String\[\]** actionParams) **throws** IOException;

  An asynchronized generic API to execute action. System will maintain an internal task to perform the action. The API will return immediately once the internal task is created.
