REST API
==========

## Config API        AuthType:HTTPS

### Add Smart rule
* Submit a HTTP POST request.
```
 https://<host>:<port>/smart/api/v1/rules/{ruleId}/add
```
Example:
```
 POST https://<host>:<port>/smart/api/v1/rules/{ruleId}/add
 Code:200
 Content-Type:application/json
```
### Delete Smart rule
* Submit a HTTP Delete request.
```
 https://<host>:<port>/smart/api/v1/rules/{ruleId}/delete
```
Example:
```
 POST https://<host>:<port>/smart/api/v1/rules/{ruleId}/delete
 Code:200
 Content-Type:application/json
```
### Start Smart rule
* Submit a HTTP POST request.
```
    https://<host>:<port>/smart/api/v1/rules/{ruleId}/start
```
Example:
```   
    POST https://<host>:<port>/smart/api/v1.0/rules/{ruleId}/start
    Code:200
    Content-Type:application/json
    Content:
        {
            "result":"success",
            "msg":"start rule{ruleId}"
        }
```
### Stop Smart rule
* Submit a HTTP DELETE request.
```
    https://<host>:<port>/smart/api/v1/rules/{ruleId}/stop
```
Example:
```
    DELETE https://<host>:<port>/smart/api/v1/rules/{ruleId}/stop
    Code:200
    Content-Type:application/json
```
### Get Smart rule detail
* Submit a HTTP GET request.
```
    https://<host>:<port>/smart/api/v1/rules/{ruleId}/detail
    Code:200
    Content-Type:application/json
    Content:
            {
                "id":1,
                "submitTime":1497921983266,
                "ruleText":"file: accessCount(1min) \u003e 1 | cache",
                "state":"ACTIVE",
                "numChecked":9,
                "numCmdsGen":0,
                "lastCheckTime":1498033813519
            }
    }
```
Example:
```   
    GET https://<host>:<port>/smart/api/v1/rules/{ruleId}/detail
    Code:200
    Content-Type:application/json
```     
### Get Smart rule status
* Submit a HTTP GET request.
```
    https://<host>:<port>/smart/api/v1/rules/{ruleId}/status
```
Example:
```
    GET https://<host>:<port>/smart/api/v1/rules/{ruleId}/errors
    Code:200
    Content-Type:application/json
```
### Get Smart rule cmdlets
* Submit a HTTP GET request.
```
    https://<host>:<port>/smart/api/v1/rules/{ruleId}/cmdlets
```
Example:
```
    GET https://<host>:<port>/smart/api/v1/rules/{ruleId}/cmdlets
    Code:200
    Content-Type:application/json
```
### Get Smart rulelist
* Submit a HTTP GET request.
```
    https://<host>:<port>/smart/api/v1.0/rules/list
```
Example:
```
    GET https://<host>:<port>/smart/api/v1/rules/list
    Code:200
    Content-Type:application/json
```
### Get Smart action summary
* Submit a HTTP GET request.
```
    https://<host>:<port>/smart/api/v1/actions/summary
```
Example:
```
    DELETE https://<host>:<port>/smart/api/v1/actions/summary
    Code:200
    Content-Type:application/json
```
### Get Smart action status
* Submit a HTTP GET request.
```
    https://<host>:<port>/smart/api/v1/actions/{actionId}/status
```
Example:
```
    DELETE https://<host>:<port>/smart/api/v1/actions/{actionId}/status
    Code:200
    Content-Type:application/json
```
### Get Smart action detail
* Submit a HTTP GET request.
```
    https://<host>:<port>/smart/api/v1/actions/{actionId}/detail
```
Example:
```
    DELETE https://<host>:<port>/smart/api/v1/actions/{actionId}/detail
    Code:200
    Content-Type:application/json
```
### Get Smart action action registry list
* Submit a HTTP GET request.
```
    https://<host>:<port>/smart/api/v1/actions/registry/list
```
Example:
```
    DELETE https://<host>:<port>/smart/api/v1/actions/registry/list
    Code:200
    Content-Type:application/json
```
### Get Smart action list
* Submit a HTTP GET request.
```
    https://<host>:<port>/smart/api/v1/actions/list
```
Example:
```
    DELETE https://<host>:<port>/smart/api/v1/actions/list
    Code:200
    Content-Type:application/json
```
### Get Smart cluster primary
* Submit a HTTP GET request.
```
    https://<host>:<port>/smart/api/v1/cluster/primary
```
Example:
```
    GET https://<host>:<port>/smart/api/v1/cluster/primary
    Code:200
    Content-Type:application/json
```
### Get Smart cached files
* Submit a HTTP GET request.
```
    https://<host>:<port>/smart/api/v1/cluster/primary/cachedfiles
```
Example:
```
    GET https://<host>:<port>/smart/api/v1/cluster/primary/cachedfiles
    Code:200
    Content-Type:application/json
```
### Get Smart hot files
* Submit a HTTP GET request.
```
    https://<host>:<port>/smart/api/v1/cluster/primary/hotfiles
```
Example:
```
    GET https://<host>:<port>/smart/api/v1/cluster/primary/hotfiles
    Code:200
    Content-Type:application/json
```
### Get Smart cluster alluxio
* Submit a HTTP GET request.
```
    https://<host>:<port>/smart/api/v1/cluster/alluxio/{clusterName}
```
Example:
```
    GET https://<host>:<port>/smart/api/v1/cluster/alluxio/{clusterName}
    Code:200
    Content-Type:application/json
```
### Get Smart cluster hdfs
* Submit a HTTP GET request.
```
    https://<host>:<port>/smart/api/v1/cluster/hdfs/{clusterName}
```
Example:
```
    GET https://<host>:<port>/smart/api/v1/cluster/hdfs/{clusterName}
    Code:200
    Content-Type:application/json
```
### Get Smart conf
* Submit a HTTP GET request.
```
    https://<host>:<port>/smart/api/v1/conf
```
Example:
```
    GET https://<host>:<port>/smart/api/v1/conf
    Code:200
    Content-Type:application/json
```
### Get Smart cmdlets status
* Submit a HTTP GET request.
```
    https://<host>:<port>/smart/api/v1/cmdlets/{cmdletId}/status
```
Example:
```
    GET https://<host>:<port>/smart/api/v1/cmdlets/1/status
    Code:200
    Content-Type:application/json
```
### Get Smart cmdlets list
* Submit a HTTP GET request.
```
    https://<host>:<port>/smart/api/v1/cmdlets/list
```
Example:
```
    GET https://<host>:<port>/smart/api/v1/cmdlets/list
    Code:200
    Content-Type:application/json
```
### Get Smart cmdlets detail
* Submit a HTTP GET request.
```
    https://<host>:<port>/smart/api/v1/cmdlets/detail
```
Example:
```
    GET https://<host>:<port>/smart/api/v1/cmdlets/detail
    Code:200
    Content-Type:application/json
```
### Get Smart cmdlets summary
* Submit a HTTP GET request.
```
    https://<host>:<port>/smart/api/v1/cmdlets/summary
```
Example:
```
    GET https://<host>:<port>/smart/api/v1/cmdlets/summary
    Code:200
    Content-Type:application/json
```
### Submit a Smart cmdlet
* Submit a HTTP POST request.
```
    https://<host>:<port>/smart/api/v1/cmdlets/submit/{actionType}
```
Example:
```
    Post https://<host>:<port>/smart/api/v1/cmdlets/submit/write?args=...
    Code:200
    Content-Type:application/json
```
### Get Smart system server
* Submit a HTTP GET request.
```
    https://<host>:<port>/smart/api/v1/system/server
```
Example:
```
    GET https://<host>:<port>/smart/api/v1/system/server
    Code:200
    Content-Type:application/json
```
### Get Smart system agent
* Submit a HTTP GET request.
```
    https://<host>:<port>/smart/api/v1/system/agent
```
Example:
```
    GET https://<host>:<port>/smart/api/v1/system/agent
    Code:200
    Content-Type:application/json
```
