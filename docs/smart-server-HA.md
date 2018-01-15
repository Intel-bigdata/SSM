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

   Since active Smart Server can run cmdlets dispatched by itself, these tasks will crush if active server is down.
   During the transition, the running Cmdlet on the new active server will also crush. SSM has been optimized to minimize the impact on user tasks during the transition.
   SSM can put key information of cmdlet and action into metastore. According to whether the status of a cmdlet in DB is dispatched or not, SSM tackles it differently. If a cmdlet is not dispatched during the transition, the new active Smart Server will reload it's info from DB and dispatch it.
   For the dispatched cmdlet, SSM can discover the crushed cmdlets by a heart beat like mechanism. The status of running cmdlet will be reported periodically.
   If the status of running actions have not been updated for a certain time, these actions will be marked as failed.


#### The configuration for SSM HA

1. In ${SMART_HOME}/config/servers, the user can add the hostname or ip address line by line to configure Smart Servers.

2. Dispatch the installation package to the Smart Servers with a same installation directory.
