/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.smartdata.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.AbstractService;
import org.smartdata.conf.SmartConf;
import org.smartdata.model.StorageCapacity;
import org.smartdata.model.Utilization;
import org.smartdata.server.cluster.NodeInfo;
import org.smartdata.server.engine.ActiveServerInfo;
import org.smartdata.server.engine.CmdletManager;
import org.smartdata.server.engine.ConfManager;
import org.smartdata.server.engine.RuleManager;
import org.smartdata.server.engine.ServerContext;
import org.smartdata.server.engine.StandbyServerInfo;
import org.smartdata.server.engine.StatesManager;
import org.smartdata.server.engine.cmdlet.HazelcastExecutorService;
import org.smartdata.server.engine.cmdlet.agent.AgentExecutorService;
import org.smartdata.server.engine.cmdlet.agent.AgentInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

public class SmartEngine extends AbstractService {
  private ConfManager confMgr;
  private SmartConf conf;
  private ServerContext serverContext;
  private StatesManager statesMgr;
  private RuleManager ruleMgr;
  private CmdletManager cmdletManager;
  private AgentExecutorService agentService;
  private HazelcastExecutorService hazelcastService;
  private List<AbstractService> services = new ArrayList<>();
  public static final Logger LOG = LoggerFactory.getLogger(SmartEngine.class);

  public SmartEngine(ServerContext context) {
    super(context);
    this.serverContext = context;
    this.conf = serverContext.getConf();
  }

  @Override
  public void init() throws IOException {
    statesMgr = new StatesManager(serverContext);
    services.add(statesMgr);
    cmdletManager = new CmdletManager(serverContext);
    services.add(cmdletManager);
    agentService = new AgentExecutorService(conf, cmdletManager);
    hazelcastService = new HazelcastExecutorService(cmdletManager);
    cmdletManager.registerExecutorService(agentService);
    cmdletManager.registerExecutorService(hazelcastService);
    ruleMgr = new RuleManager(serverContext, statesMgr, cmdletManager);
    services.add(ruleMgr);

    for (AbstractService s : services) {
      s.init();
    }
  }

  @Override
  public boolean inSafeMode() {
    if (services.isEmpty()) { //Not initiated
      return true;
    }
    for (AbstractService service : services) {
      if (service.inSafeMode()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void start() throws IOException {
    for (AbstractService s : services) {
      s.start();
    }
  }

  @Override
  public void stop() throws IOException {
    for (int i = services.size() - 1; i >= 0; i--) {
      stopEngineService(services.get(i));
    }
  }

  private void stopEngineService(AbstractService service) {
    try {
      if (service != null) {
        service.stop();
      }
    } catch (IOException e) {
      LOG.error("Error while stopping "
          + service.getClass().getCanonicalName(), e);
    }
  }

  public List<StandbyServerInfo> getStandbyServers() {
    return hazelcastService.getStandbyServers();
  }

  public List<AgentInfo> getAgents() {
    return agentService.getAgentInfos();
  }

  public ConfManager getConfMgr() {
    return confMgr;
  }

  public SmartConf getConf() {
    return serverContext.getConf();
  }

  public StatesManager getStatesManager() {
    return statesMgr;
  }

  public RuleManager getRuleManager() {
    return ruleMgr;
  }

  public CmdletManager getCmdletManager() {
    return cmdletManager;
  }

  public Utilization getUtilization(String resourceName) throws IOException {
    return getStatesManager().getStorageUtilization(resourceName);
  }

  public List<Utilization> getHistUtilization(String resourceName, long granularity,
      long begin, long end) throws IOException {
    long now = System.currentTimeMillis();
    if (begin == end && Math.abs(begin - now) <= 5) {
      return Arrays.asList(getUtilization(resourceName));
    }

    List<StorageCapacity> cs = serverContext.getMetaStore().getStorageHistoryData(
        resourceName, granularity, begin, end);
    List<Utilization> us = new ArrayList<>(cs.size());
    for (StorageCapacity c : cs) {
      us.add(new Utilization(c.getTimeStamp(), c.getCapacity(), c.getUsed()));
    }
    return us;
  }

  private List<Utilization> getFackData(String resourceName, long granularity,
      long begin, long end) {
    List<Utilization> utils = new ArrayList<>();
    long ts = begin;
    if (ts % granularity != 0) {
      ts += granularity;
      ts = (ts / granularity) * granularity;
    }
    Random rand = new Random(ts);

    for (; ts <= end; ts += granularity) {
      utils.add(new Utilization(ts, 100, rand.nextInt(100)));
    }
    return utils;
  }

  public List<NodeInfo> getSsmNodesInfo() {
    List<NodeInfo> ret = new LinkedList<>();
    ret.addAll(Arrays.asList(ActiveServerInfo.getInstance()));
    ret.addAll(getStandbyServers());
    ret.addAll(getAgents());
    return ret;
  }
}
