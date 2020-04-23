
## **Remote Debug**

### Active Smart Server

Specify debug option for active smart server when starting a SSM cluster.

`./bin/start-ssm.sh --debug master`

### Standby Smart Server

Start a standby smart server in debug mode after active smart server is ready.

`./bin/start-standby.sh --host {HOST_NAME} --debug`

Alternatively, if there is only one standby server configured, debug option can be specified for standby server when starting a SSM cluster.

`./bin/start-ssm.sh --debug standby`


### Smart Agent

Start a smart agent in debug mode after active smart server is ready.

`./bin/start-agent --host {HOST_NAME} --debug`

Alternatively, if there is only one agent configured, debug option can be specified for agent when starting a SSM cluster.

`./bin/start-ssm --debug agent`


### IDE Debug Configuration

Set IDE remote debug configuration for one of the above three kinds of processes in debug mode. Take Intellij as example.

* Add remote debug: Run -> Edit Configuration -> + -> Remote. And change the setting as the following shows.

  Debugger mode: attach

  Host: {HOSTNAME}

  Port: 8008

  The above host is the one running the above three kinds of processes in debug mode.

* Start the above remote debug in IDE.

* If a proxy server bridges your local computer and remote server, you can consider to use the below command to forward
socket message to your local computer 8008 port. And you also need to change the remote debug setting by replacing remote
server host name with your local computer's host name .

  `ssh -L 8008:{REMOTE_SERVER}:8008 {USER}@{PROXY_HOST} -N`