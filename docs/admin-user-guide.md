<span id="_Toc392169190" class="anchor"><span id="_Toc415575594" class="anchor"></span></span>Admin API
==========================================================================================================

This document introduces the definition of APIs exposed by Intel Smart
Storage Management (SSM) to make seamless integration with other Hadoop
components.

There are two sets of APIs, Admin APIs and Application APIs. Admin APIs
are used by Hadoop cluster administrators who are responsible for managing
SSM rules. This set of APIs includes create/delete/list/update SSM
rules. Hadoop super user privilege is required to access Admin APIs.

Application APIs are used by applications running on top of HDFS. This set
of APIs include move/archive/cache file level operations. The system will
execute the file operations on behalf of the application, with the privilege of
the user who started the application.

If Application API and Admin API are in conflict, for example, they
want to execute different operations on the same file at the same time,
Application API will precede Admin API. Application API operation will
succeed and Admin API operation on the same file will be cancelled. This
rule is based on the assumption that application knows more about its
data (files) than the cluster administrator.

For easily integration, the APIs are exposed via both RPC and RESTful interfaces. Users can choose the one which fits their environment.

<img src="./image/api.png" width="461" height="295" />

Define Rule
-----------

<span id="_Add_Reports" class="anchor"></span>A rule defines all the
things for SSM to work, what kind of data metrics are involved, what
conditions, at what time which actions should be taken when the
conditions are true. By writing rules, a user can easily manage their
cluster and adjust its behavior for certain purposes.

User need to define the rule first based on the requirements. A rule
has the following format,

<img src="./image/rule-syntax.png" width="481" height="208" />

A rule contains four parts, Object to manipulate, trigger, conditions
and commands. “:” and “|” are used as the separator to separate
different rule part.

Detailed information for each rule part is listed in the following tables.

Table - 1 Objects to manipulate

| Object  | Description       | Example                            |
|---------|-------------------|------------------------------------|
| file    | Files             | *file with path matches "/fooA/\*.dat"* |

Table - 2 Triggers

| Format                                | Description                                             | Example                               |
|---------------------------------------|---------------------------------------------------------|---------------------------------------|
| at &lt;time&gt;                       | Execute the rule at the given time                      | - at “2017-07-29 23:00:00” <br> -   at now |
| every &lt;time interval&gt;           | Execute the rule at the given frequency                 | - every 1min                            |
| from &lt;time&gt; \[To &lt;time&gt;\] | Along with ‘every’ expression to specify the time scope | - every 1day from now <br>  -   every 1min from now to now + 7day  |


Table – 3 Conditions

| Ingredient       | Description                                                                              | Example                                  |
|------------------|------------------------------------------------------------------------------------------|------------------------------------------|
| Object property  | Object property as condition subject, refer to table-4 to supported object property list | - length &gt; 5MB                          |
| Time             | - “yyyy-MM-dd HH:mm:ss:ms” <br>  -   Predefined <br>  -   Time + Time Interval           | - “2017-07-29 23:00:00” <br>  -   now  <br>  -   now + 7day  |
| Time Interval    | - Digital + unit <br> -   Time – Time <br> -   Time Interval + Time Interval             | - 5ms, 5sec, 5min, 5hour, 5day <br>  -   now - “2016-03-19 23:00:00” <br>  -   5hour + 5min           |
| File Size        | - Digital + unit                                                                         | - 5B, 5kb, 5MB, 5GB, 5TB, 5PB              |
| String           | Start and ends with “, support escapes                                                   | - “abc”, “123”, “Hello world\\n”           |
| Logical operator | and, or, not                                                                             |                                          |
| Digital operator | +, -, \*, /, %                                                                           |                                          |
| Compare          | &gt;, &gt;=, &lt;, &lt;=, ==, !=                                                         |                                          |

Table – 4 Object properties

| Object   | Property                 | Description                                    |
|----------|--------------------------|------------------------------------------------|
| file     | path                     | Path in HDFS                                   |
|          | age                      | Time from last been modified                   |
|          | atime                    | Time accessed last time                        |
|          | accessCount(Time Interval)    | Access counts during the last time interval |
|          | blocksize                | Block size of the file                         |
|          | storagePolicy            | Storage policy of file                         |
|          | length                   | Length of the file                             |
|          | inCache                  | Test if file is in cache now                   |
|          | isDir                    | Test if file is a directory                    |
|          | mtime                    | Last modification time of the file             |

Table – 5 Commands

| Command(case insensitive) | Description                                               |
|---------------------------|-----------------------------------------------------------|
| cache                     | Cache file in HDFS Cache                                  |
| uncache                   | Uncache file                                              |
| onessd                    | Move one copy of file to SSD                              |
| allssd                    | Move all copies of file to SSD                            |
| onedisk                   | Move one copy of file to DISK                             |
| alldisk                   | Move all copies of file to DISK                           |
| archive                   | Move file to ‘Archive’ storage type                       |
| ramdisk                   | Store one copy of file to RAM_DISK                        |
| checkstorage              | Check file block storage type                             |
| read                      | Read the file and discard the content read                |
| write                     | Create the file and fill with random values               |
| sleep                     | Sleep for given time                                      |
| sync                      | Sync file to remote cluster                               |
| user defined actions      | Interface defined for user to implement their own actions |

Here is a rule example,

*file with path matches "/fooA/\*.dat": age &gt; 30day | archive*

This example defines a rule that for each file (specified before key word 'with') with path matches regular
expression “/fooA/\*.dat”, if the file has been created for more than 30
days then move the file to archive storage. The rule can be rewrited in the following way:

*file : path matches "/fooA/\*.dat" and age &gt; 30day | archive*

The boolean expression can also be placed in condition expression.

For those who not sure if the rule is defined correctly or not, an API
is provided to check whether the rule is valid or not. Please refer to
the Rule API section for detailed API information.

Use Rule
--------

A rule has 4 states in the system: active, disabled, finished, and
deleted. Here is the rule state transition diagram.

<img src="./image/rule-state.png" width="461" height="295" />

**Active**:

Once a rule is defined and submitted to SSM, the rule is in “**Active”**
state. When a rule is in this state, SSM will regularly evaluate the
conditions of the rule, create commands when the conditions are met and
execute the commands. Once a rule finishes (rules that are only  checked at a
given time or within a time interval), the rule will transition to “**Finished**” state.

**Disabled**:

User can disable an **“Active”** rule if he/she wants to pause the
evaluation of the rule for the time being. Later if the user wants to enable the
rule again, he/she can re-activate the rule, to continue the evaluation of
the rule conditions. If there are as yet unexecuted commands associated with a rule when the user at the point in time when it was disabled, the user can choose to cancel these not unexecuted commands or
continue to process/finish them. By default, the  unexecuted commands will
be cancelled.

**Finished**:

If a rule is a one-shot rule or a time-constrained rule whose time is past, the rule enters the “**Finished**” state. A finished rule can
be deleted permanently from the system.

**Deleted:**

It’s an ephemeral state of a rule. A rule in this state means the rule
has already been deleted by user, but there are pending commands of this
rule that user would like should run to completion. Once all pending commands are
finished, the rule will be permanently deleted from the system.

Rule Management API
-------------------

Rule management API is provided for both the RPC and RESTful HTTP interfaces.
Here is the RPC interface definition. RESTFull HTTP interface will be
updated later.

 
* long **submitRule**(**String** rule) **throws** IOException;  

  Submit a rule  

* long **submitRule**(**String** rule, **RuleState** initState) **throws** IOException;

  Submit a rule with specified initial state. The initial state can be “active” or “disabled”. If initial state is not specified, then by default rule will be of “active” state.

* void **checkRule**(**String** rule) **throws** IOException;

  Verify rule

* RuleInfo **getRule**(**long** ruleID) **throws** IOException;  

  Get rule information

* List&lt;RuleInfo&gt; **listRules**() **throws** IOException;  

  List all current rules in the system, including active, disabled, finished, or deleted.

* void **deleteRule**(**long** ruleID, **boolean** dropPendingCommands) **throws** IOException;

  Delete a rule. If dropPendingCommands is false then the rule will still be kept in the system with “deleted” state. Once all the pending commands are finished then the rule will be deleted. Only “disabled” or “finished” rule can be deleted.  

* void **enableRule**(**long** ruleID) **throws** IOException;
 
  Enable a rule. Only “disabled” rules can be enabled. Enabling a rule in another state rule will throw exception.

* void **disableRule**(**long** ruleID, **boolean** dropPendingCommands) **throws** IOException;
 
  Disable a rule. If dropPendingCommands is false then the rule will still be marked as “disabled” state while all the pending commands continue to execute to finish. Only an “active” rule can be disabled.
