# The design of Smart Server HA

SSM supports the configuration of multiply Smart Servers to achieve high availability.

#### The details of SSM HA

1. Multiple servers

   Multiply servers can be configured as Smart Server, but only one is active in the cluster.
   The others are standby servers. Like an agent, the standby server just provides Cmdlet Executor service which executes the work assigned by active server.

   ![](https://github.com/Intel-Bigdata/SSM/blob/trunk/docs/image/HA-active.png)

   SSM employs hazelcast’s master election mechanism to manage multiply Smart Servers. If active server is down, a standby server is elected as an active one and then it will launch all required services.
   Each Smart Agent keeps a list of all Smart Server hosts. So it can find the active server by trying to connecting to these hosts.

   ![](https://github.com/Intel-Bigdata/SSM/blob/trunk/docs/image/HA-transition.png)

2. HA Transition

   When active server is down, a standby server will take at lease several seconds to transform its role to active server.
   During this transition, we try to minimize the impact on some user's tasks. Two kinds of cmdlets are considered by SSM.
   Their infomation is flushed to DB.

   1. The pending cmdlet.

      The pending cmdlet has not been dispatched to a server to execute. The new active server will reload these cmdlets from DB
      and dispatch them.

   2. The dispatched cmdlet.

      A cmdlet can be dispatched to active server, standby server and agent.
      If a cmdlet is dispatched to active server. It will terminate when the active server is down.
      If a cmdlet is running on the standby server which transforms to active server, it will also terminate during the transition.
      If a cmdlet is running on agent, it can be executed successfully.

      For the dispatched cmdlet, SSM can discover the crushed cmdlet by a heart beat like mechanism.
      The status of a dispatched cmdlet will be reported periodically till it is finished.
      If the status of actions belonging to a dispatched cmdlet has not been reported for a certain time,
      these actions will be marked as failed. A timeout log is attached to these actions.

#### The configuration for SSM HA

1. In ${SMART_HOME}/config/servers, the user can add the hostname or ip address line by line to configure Smart Servers.

2. Dispatch the installation package to the Smart Servers with a same installation directory.
