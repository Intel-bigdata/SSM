Application API
===============

Application APIs are used by applications run on top of HDFS. This set
of APIs include move/archive/cache file level operation etc. System will
execute the file operation on half of application, with the privilege of
the user who starts the application. SSM will provide a SmartClient
which includes both HDFS DFSClient functions and new SSM Application
APIs. Upper level application can use this SmartClient instead of
original HDFS DFSClient. Here is the diagram.

<img src="./client.png" width="554" height="408" />

Command API
-----------

/*  
\*Cache a file<br>
*/  
void **cache**(**String** filePath) **throws** IOException;

/*  
\*Uncache a file<br>
*/  
void **uncache**(**String** filePath) **throws** IOException;

/*<br>
\*Move a file to destination<br>
*/  
void **move**(**String** srcFilePath, **String** destPath) **throws**
IOException;

/*  
\* List all command name currently supported by the system. <br>
\* Current supported command name are “move”, “cache”, “uncache” etc. <br>
*/  
String\[\] **getCommandList**() **throws** IOException;

/* <br>
\* A synchronized generic API to execute command. <br>
\* System will maintain an internal task to performance the action. The API will return until the task is finished.  <br>
*/  
void **executeCommand**(**String** cmdName, **String\[\]** cmdParams)
**throws** IOException;

/*  
\* A asynchronized generic API to execute command.
\* System will maintain an internal task to performance the action. The API will return immediately once the internal task is created.<br>
*/  
void **executeCommandAsync**(**String** cmdName, **String\[\]** cmdParams)
**throws** IOException;
