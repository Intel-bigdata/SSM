/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.smartdata.server.engine;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.AbstractService;
import org.smartdata.actions.ActionRegistry;
import org.smartdata.actions.ActionStatus;
import org.smartdata.actions.SmartAction;
import org.smartdata.actions.hdfs.HdfsAction;
import org.smartdata.client.SmartDFSClient;
import org.smartdata.common.CmdletState;
import org.smartdata.common.actions.ActionDescriptor;
import org.smartdata.common.models.ActionInfo;
import org.smartdata.common.actions.ActionInfoComparator;
import org.smartdata.common.cmdlet.CmdletDescriptor;
import org.smartdata.common.models.CmdletInfo;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.metastore.MetaStore;
import org.smartdata.server.engine.cmdlet.Cmdlet;
import org.smartdata.server.engine.cmdlet.CmdletPool;
import org.smartdata.server.utils.HadoopUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Schedule and execute cmdlets passed down.
 */
public class CmdletExecutor extends AbstractService implements Runnable {
  static final Logger LOG = LoggerFactory.getLogger(CmdletExecutor.class);

  private ServerContext serverContext;
  private MetaStore metaStore;

  private ArrayList<Set<Long>> cmdsInState = new ArrayList<>();
  private Map<Long, CmdletInfo> cmdsAll = new ConcurrentHashMap<>();
  // TODO replace with concurrentSet or MAP
  private Set<CmdTuple> statusCache;
  private Daemon cmdletExecutorThread;
  private CmdletPool cmdletPool;
  private Map<String, Long> cmdletHashSet;
  private Map<String, Long> fileLock;
  private Map<Long, SmartAction> actionPool;
  private boolean running;
  private long maxActionId;
  private long maxCmdletId;

  public CmdletExecutor(ServerContext context) {
    super(context);

    this.serverContext = context;
    this.metaStore = context.getMetaStore();
    
    statusCache = new HashSet<>();
    for (CmdletState s : CmdletState.values()) {
      cmdsInState.add(s.getValue(), new HashSet<Long>());
    }

    cmdletPool = new CmdletPool();
    actionPool = new ConcurrentHashMap<>();
    cmdletHashSet = new ConcurrentHashMap<>();
    fileLock = new ConcurrentHashMap<>();
    running = false;
  }

  @Override
  public void init() throws IOException {
      try {
        maxActionId = serverContext.getMetaStore().getMaxActionId();
        maxCmdletId = serverContext.getMetaStore().getMaxCmdletId();
      } catch (Exception e) {
        maxActionId = 1;
        LOG.error("DB Connection error! Get Max CmdletId/ActionId fail!", e);
        throw new IOException(e);
      }
  }

  /**
   * Start CmdletExecutor.
   */
  @Override
  public void start() throws IOException {
    // TODO add recovery code
    cmdletExecutorThread = new Daemon(this);
    cmdletExecutorThread.setName(this.getClass().getCanonicalName());
    cmdletExecutorThread.start();
    running = true;
  }

  /**
   * Stop CmdletExecutor
   */
  @Override
  public void stop() throws IOException {
    running = false;

    try {
      if (cmdletPool != null) {
        cmdletPool.stop();
      }
    } catch (Exception e) {
      LOG.error("Shutdown MoverPool/CmdletPool Error!");
      throw new IOException(e);
    }

    // Update Status
    batchCmdletStatusUpdate();
  }

  @Override
  public void run() {
    while (running) {
      try {
        // control the cmdlets that executed concurrently
        if (cmdletPool == null) {
          LOG.error("Thread Init/Start Error!");
        }
        // TODO: use configure value
        if (cmdletPool.size() <= 5) {
          Cmdlet toExec = schedule();
          if (toExec != null) {
            toExec.setScheduleToExecuteTime(Time.now());
            cmdletPool.execute(toExec);
          } else {
            Thread.sleep(1000);
          }
        } else {
          Thread.sleep(1000);
        }
      } catch (InterruptedException e) {
        if (!running) {
          break;
        }
      } catch (IOException e) {
        LOG.error("Schedule error!", e);
      }
    }
  }

  public ServerContext getContext() {
    return serverContext;
  }

  public CmdletInfo getCmdletInfo(long cid) throws IOException {
    if (cmdsAll.containsKey(cid)) {
      return cmdsAll.get(cid);
    }
    List<CmdletInfo> ret = null;
    try {
      ret = metaStore.getCmdletsTableItem(String.format("= %d", cid),
          null, null);
    } catch (SQLException e) {
      LOG.error("Get CmdletInfo with ID {} from DB error! {}", cid, e);
      throw new IOException(e);
    }
    if (ret != null) {
      return ret.get(0);
    }
    return null;
  }

  public List<CmdletInfo> listCmdletsInfo(long rid,
      CmdletState cmdletState) throws IOException {
    List<CmdletInfo> retInfos = new ArrayList<>();
    // Get from DB
    try {
      if (rid != -1) {
        retInfos.addAll(metaStore.getCmdletsTableItem(null,
            String.format("= %d", rid), cmdletState));
      } else {
        retInfos.addAll(metaStore.getCmdletsTableItem(null,
            null, cmdletState));
      }
    } catch (SQLException e) {
      LOG.error("List CmdletInfo from DB error! Conditions rid {}, {}", rid, e);
      throw new IOException(e);
    }
    // Get from CacheObject if cmdletState != CmdletState.PENDING
    if (cmdletState != CmdletState.PENDING) {
      for (Iterator<CmdletInfo> iter = cmdsAll.values().iterator(); iter.hasNext(); ) {
        CmdletInfo cmdinfo = iter.next();
        if (cmdinfo.getState() == cmdletState && cmdinfo.getRid() == rid) {
          retInfos.add(cmdinfo);
        }
      }
    }
    return retInfos;
  }

  public void activateCmdlet(long cid) throws IOException {
    if (inCache(cid)) {
      return;
    }
    if (inUpdateCache(cid)) {
      return;
    }
    CmdletInfo cmdinfo = getCmdletInfo(cid);
    if (cmdinfo == null || cmdinfo.getState() == CmdletState.DONE) {
      return;
    }
    LOG.info("Activate Cmdlet {}", cid);
    cmdinfo.setState(CmdletState.PENDING);
    addToPending(cmdinfo);
  }

  public void disableCmdlet(long cid) throws IOException {
    // Remove from CacheObject
    if (inCache(cid)) {
      LOG.info("Disable Cmdlet {}", cid);
      // Cmdlet is finished, then return
      if (inUpdateCache(cid)) {
        return;
      }
      CmdletInfo cmdinfo = cmdsAll.get(cid);
      cmdinfo.setState(CmdletState.DISABLED);
      // Disable this cmdlet in cache
      if (inExecutingList(cid)) {
        // Remove from Executing queue
        removeFromExecuting(cid, cmdinfo.getRid());
        // Kill thread
        cmdletPool.deleteCmdlet(cid);
      } else {
        // Remove from Pending queue
        cmdsInState.get(CmdletState.PENDING.getValue()).remove(cid);
      }
      // Mark as cancelled, this status will be update to DB
      // in next batch update
      synchronized (statusCache) {
        statusCache.add(new CmdTuple(cid, cmdinfo.getRid(),
            CmdletState.DISABLED));
      }
    }
  }

  public void deleteCmdlet(long cid) throws IOException {
    // Delete from DB
    // Remove from CacheObject
    if (inCache(cid)) {
      // Cmdlet is finished, then return
      CmdletInfo cmdinfo = cmdsAll.get(cid);
      // Disable this cmdlet in cache
      if (inExecutingList(cid)) {
        // Remove from Executing queue
        removeFromExecuting(cid, cmdinfo.getRid());
        // Kill thread
        cmdletPool.deleteCmdlet(cid);
      } else if (inUpdateCache(cid)) {
        removeFromUpdateCache(cid);
      } else {
        // Remove from Pending queue
        cmdsInState.get(CmdletState.PENDING.getValue()).remove(cid);
      }
      // Mark as cancelled, this status will be update to DB
      // in next batch update
      cmdsAll.remove(cid);
    }
    try {
      metaStore.deleteCmdlet(cid);
    } catch (SQLException e) {
      LOG.error("Delete Cmdlet {} from DB error! {}", cid, e);
      throw new IOException(e);
    }
  }

  public ActionInfo getActionInfo(long actionID) throws IOException {
    ActionInfo actionInfo = null;
    ActionInfo dbActionInfo = null;
    try {
      dbActionInfo = metaStore.getActionsTableItem(
          String.format("== %d ", actionID), null).get(0);
    } catch (SQLException e) {
      LOG.error("Get ActionInfo of {} from DB error! {}",
          actionID, e);
      throw new IOException(e);
    }
    if (dbActionInfo.isFinished()) {
      return dbActionInfo;
    }
    SmartAction smartAction = actionPool.get(actionID);
    if (smartAction != null) {
      actionInfo = createActionInfoFromAction(smartAction, 0);
    }
    if (actionInfo == null) {
      return dbActionInfo;
    }
    actionInfo.setCmdletId(dbActionInfo.getCmdletId());
    return actionInfo;
  }

  /**
   * List actions supported in SmartServer.
   *
   * @return
   * @throws IOException
   */
  public List<ActionDescriptor> listActionsSupported() throws IOException {
    //TODO add more information for list ActionDescriptor
    ArrayList<ActionDescriptor> actionDescriptors = new ArrayList<>();
    for (String name : ActionRegistry.namesOfAction()) {
      actionDescriptors.add(new ActionDescriptor(name,
          name, "", ""));
    }
    return actionDescriptors;
  }

  private boolean isActionSupported(String actionName) {
    return ActionRegistry.checkAction(actionName);
  }

  private void addToPending(CmdletInfo cmdinfo) throws IOException {
    Set<Long> cmdsPending = cmdsInState.get(CmdletState.PENDING.getValue());
    cmdsAll.put(cmdinfo.getCid(), cmdinfo);
    cmdsPending.add(cmdinfo.getCid());
  }

  public int cacheSize() {
    return cmdsAll.size();
  }

  public boolean inCache(long cid) throws IOException {
    return cmdsAll.containsKey(cid);
  }

  public boolean inExecutingList(long cid) throws IOException {
    Set<Long> cmdsExecuting = cmdsInState
        .get(CmdletState.EXECUTING.getValue());
    return cmdsExecuting.contains(cid);
  }

  public boolean inPendingList(long cid) throws IOException {
    Set<Long> cmdsPending = cmdsInState.get(CmdletState.PENDING.getValue());
    LOG.info("Size of Pending = {}", cmdsPending.size());
    return cmdsPending.contains(cid);
  }

  public boolean inUpdateCache(long cid) throws IOException {
    if (statusCache.size() == 0) {
      return false;
    }
    for (CmdTuple ct : statusCache) {
      if (ct.cid == cid) {
        return true;
      }
    }
    return false;
  }

  private void removeFromUpdateCache(long cid) throws IOException {
    if (statusCache.size() == 0) {
      return;
    }
    synchronized (statusCache) {
      for (Iterator<CmdTuple> iter = statusCache.iterator(); iter.hasNext(); ) {
        CmdTuple ct = iter.next();
        if (ct.cid == cid) {
          iter.remove();
          break;
        }
      }
    }
  }

  /**
   * Get cmdlet to for execution.
   *
   * @return
   */
  private synchronized Cmdlet schedule() throws IOException {
    // currently FIFO
    Set<Long> cmdsPending = cmdsInState
        .get(CmdletState.PENDING.getValue());
    Set<Long> cmdsExecuting = cmdsInState
        .get(CmdletState.EXECUTING.getValue());
    if (cmdsPending.size() == 0) {
      // Put them into cmdsAll and cmdsInState
      if (statusCache.size() != 0) {
        batchCmdletStatusUpdate();
      }
      List<CmdletInfo> dbcmds = getPendingCmdletsFromDB();
      if (dbcmds == null) {
        return null;
      }
      for (CmdletInfo c : dbcmds) {
        // if cmdlet alread in update cache or queue then skip
        if (cmdsAll.containsKey(c.getCid())) {
          continue;
        }
        cmdsAll.put(c.getCid(), c);
        cmdsPending.add(c.getCid());
      }
      if (cmdsPending.size() == 0) {
        return null;
      }
    }
    // TODO Replace FIFO
    // Currently only get and run the first cmd
    long curr = cmdsPending.iterator().next();
    CmdletInfo cmdletInfo = cmdsAll.get(curr);
    Cmdlet ret = getCmdletFromCmdInfo(cmdletInfo);
    cmdsPending.remove(curr);
    if (ret == null) {
      // Create Cmdlet from CmdletInfo Fail
      LOG.error("Create Cmdlet from CmdletInfo {}", cmdletInfo);
      statusCache.add(new CmdTuple(cmdletInfo.getCid(), cmdletInfo.getRid(), CmdletState.DISABLED));
      return null;
    }
    cmdsExecuting.add(curr);
    ret.setState(CmdletState.EXECUTING);
    return ret;
  }

  private SmartAction createSmartAction(ActionInfo actionInfo) throws IOException {
    SmartAction smartAction = ActionRegistry.createAction(actionInfo.getActionName());
    if (smartAction == null) {
      return null;
    }
    smartAction.setContext(serverContext);
    smartAction.setArguments(actionInfo.getArgs());
    if (smartAction instanceof HdfsAction) {
      ((HdfsAction) smartAction).setDfsClient(
          new SmartDFSClient(HadoopUtils.getNameNodeUri(serverContext.getConf()),
              serverContext.getConf(), getRpcServerAddress()));
    }
    smartAction.getActionStatus().setId(actionInfo.getActionId());
    return smartAction;
  }

  private SmartAction createSmartAction(String name) throws IOException {
    SmartAction smartAction = ActionRegistry.createAction(name);
    if (smartAction == null) {
      return null;
    }
    smartAction.setContext(serverContext);
    if (smartAction instanceof HdfsAction) {
      ((HdfsAction) smartAction).setDfsClient(
          new SmartDFSClient(HadoopUtils.getNameNodeUri(serverContext.getConf()),
              serverContext.getConf(), getRpcServerAddress()));
    }
    smartAction.getActionStatus().setId(maxActionId);
    maxActionId++;
    return smartAction;
  }

  private InetSocketAddress getRpcServerAddress() {
    String[] strings = serverContext.getConf().get(SmartConfKeys.DFS_SSM_RPC_ADDRESS_KEY,
        SmartConfKeys.DFS_SSM_RPC_ADDRESS_DEFAULT).split(":");
    return new InetSocketAddress(strings[strings.length - 2]
        , Integer.parseInt(strings[strings.length - 1]));
  }

  private List<ActionInfo> createActionInfos(String cmdletDescriptorString, long cid) throws IOException {
    CmdletDescriptor cmdletDescriptor = null;
    try {
      cmdletDescriptor = CmdletDescriptor.fromCmdletString(cmdletDescriptorString);
    } catch (ParseException e) {
      LOG.error("Cmdlet Descriptor {} String Wrong format! {}", cmdletDescriptorString, e);
    }
    return createActionInfos(cmdletDescriptor, cid);
  }


  @VisibleForTesting
  public synchronized List<ActionInfo> createActionInfos(CmdletDescriptor cmdletDescriptor, long cid) throws IOException {
    if (cmdletDescriptor == null) {
          return null;
    }
    List<ActionInfo> actionInfos = new ArrayList<>();
    ActionInfo current;
    // Check if any files are in fileLock
    for (int index = 0; index < cmdletDescriptor.actionSize(); index++) {
      Map<String, String> args = cmdletDescriptor.getActionArgs(index);
      if (args != null && args.size() >= 1) {
        String file = args.get(CmdletDescriptor.HDFS_FILE_PATH);
        if (file != null && fileLock.containsKey(file)) {
          LOG.warn("Warning: Other actions are processing {}!", file);
          throw new IOException();
        }
      }
    }
    // Create actioninfos and add file to file locks
    for (int index = 0; index < cmdletDescriptor.actionSize(); index++) {
      Map<String, String> args = cmdletDescriptor.getActionArgs(index);
      current = new ActionInfo(maxActionId, cid,
          cmdletDescriptor.getActionName(index),
          args, "", "",
          false, 0, false, 0, 0);
      maxActionId++;
      actionInfos.add(current);
    }
    return actionInfos;
  }

  public synchronized long submitCmdlet(String cmdletDescriptorString)
      throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Received Cmdlet -> [" + cmdletDescriptorString + "]");
    }
    if (cmdletHashSet.containsKey(cmdletDescriptorString)) {
      LOG.debug("Duplicate Cmdlet found, submit canceled!");
      throw new IOException();
      // return -1;
    }
    CmdletDescriptor cmdletDescriptor;
    try {
      cmdletDescriptor = CmdletDescriptor.fromCmdletString(cmdletDescriptorString);
    } catch (ParseException e) {
      LOG.error("Cmdlet Descriptor {} Wrong format! {}", cmdletDescriptorString, e);
      throw new IOException(e);
    }
    return submitCmdlet(cmdletDescriptor);
  }

  public synchronized long submitCmdlet(CmdletDescriptor cmdletDescriptor)
      throws IOException {
    if (cmdletDescriptor == null) {
      LOG.error("Cmdlet Descriptor!");
      throw new IOException();
      // return -1;
    }
    if (cmdletHashSet.containsKey(cmdletDescriptor.getCmdletString())) {
      LOG.debug("Duplicate Cmdlet found, submit canceled!");
      throw new IOException();
      // return -1;
    }
    long submitTime = System.currentTimeMillis();
    CmdletInfo cmdinfo = new CmdletInfo(maxCmdletId, cmdletDescriptor.getRuleId(),
        CmdletState.PENDING, cmdletDescriptor.getCmdletString(),
        submitTime, submitTime);
    maxCmdletId ++;
    for (int index = 0; index < cmdletDescriptor.actionSize(); index++) {
      if (!ActionRegistry.checkAction(cmdletDescriptor.getActionName(index))) {
        LOG.error("Submit Cmdlet {} error! Action names are not correct!", cmdinfo);
        throw new IOException();
      }
    }
    return submitCmdlet(cmdinfo);
  }

  public synchronized long submitCmdlet(CmdletInfo cmdinfo) throws IOException {
    long cid = cmdinfo.getCid();
    List<ActionInfo> actionInfos = createActionInfos(cmdinfo.getParameters(), cid);
    for (ActionInfo actionInfo: actionInfos) {
      cmdinfo.addAction(actionInfo.getActionId());
    }
    try {
      // Insert Cmdlet into DB
      metaStore.insertCmdletTable(cmdinfo);
    } catch (SQLException e) {
      LOG.error("Submit Cmdlet {} to DB error! {}", cmdinfo, e);
      throw new IOException(e);
    }
    try {
      // Insert Action into DB
      metaStore.insertActionsTable(actionInfos.toArray(new ActionInfo[actionInfos.size()]));
    } catch (SQLException e) {
      LOG.error("Submit Actions {} to DB error! {}", actionInfos, e);
      try {
        metaStore.deleteCmdlet(cmdinfo.getCid());
      } catch (SQLException e1) {
        LOG.error("Recover/Delete Cmdlet {} rom DB error! {}", cmdinfo, e);
      }
      throw new IOException(e);
    }
    cmdletHashSet.put(cmdinfo.getParameters(), cmdinfo.getCid());
    for (ActionInfo actionInfo: actionInfos) {
      Map<String, String> args = actionInfo.getArgs();
      String file = args.get(CmdletDescriptor.HDFS_FILE_PATH);
      if (file != null) {
        fileLock.put(file, actionInfo.getActionId());
      }
    }
    return cid;
  }

  private Cmdlet getCmdletFromCmdInfo(CmdletInfo cmdinfo)
      throws IOException {
    // New Cmdlet
    Cmdlet cmd;
    List<ActionInfo> actionInfos;
    try {
      actionInfos = metaStore.getActionsTableItem(cmdinfo.getAids());
    } catch (SQLException e) {
      LOG.error("Get Actions from DB with IDs {} error!", cmdinfo.getAids());
      throw new IOException(e);
    }
    if (actionInfos == null || actionInfos.size() == 0){
      return null;
    }
    List<SmartAction> smartActions = new ArrayList<>();
    for (ActionInfo actionInfo: actionInfos) {
      SmartAction smartAction = createSmartAction(actionInfo);
      smartActions.add(smartAction);
      actionPool.put(actionInfo.getActionId(), smartAction);
    }
    if (smartActions.size() == 0) {
      return null;
    }
    cmd = new Cmdlet(smartActions.toArray(new SmartAction[smartActions.size()]),
        new Callback(), metaStore);
    cmd.setParameters(cmdinfo.getParameters());
    cmd.setId(cmdinfo.getCid());
    cmd.setRuleId(cmdinfo.getRid());
    cmd.setState(cmdinfo.getState());
    // Init action
    return cmd;
  }

  public List<ActionInfo> listNewCreatedActions(int maxNumActions)
      throws IOException {
    ArrayList<ActionInfo> actionInfos = new ArrayList<>();
    for (Cmdlet cmd : cmdletPool.getcmdlets()) {
      long cmdId = cmd.getId();
      for (SmartAction smartAction : cmd.getActions()) {
        actionInfos.add(createActionInfoFromAction(smartAction, cmdId));
      }
    }
    // Sort and get top maxNumActions
    Collections.sort(actionInfos, new ActionInfoComparator());
    if (maxNumActions <= actionInfos.size()) {
      return actionInfos.subList(0, maxNumActions);
    }
    // Get actions from Db
    int remainsAction = maxNumActions - actionInfos.size();
    try {
      actionInfos.addAll(metaStore.getNewCreatedActionsTableItem(remainsAction));
    } catch (SQLException e) {
      LOG.error("Get Finished Actions from DB error", e);
      throw new IOException(e);
    }
    return actionInfos;
  }

  public List<CmdletInfo> getPendingCmdletsFromDB() throws IOException {
    // Get Pending cmds from DB
    try {
      return metaStore.getCmdletsTableItem(null,
          null, CmdletState.PENDING);
    } catch (SQLException e) {
      LOG.error("Get Pending Cmdlets From DB error!", e);
      throw new IOException(e);
    }
  }

  public Long[] getCmdlets(CmdletState state) {
    Set<Long> cmds = cmdsInState.get(state.getValue());
    return cmds.toArray(new Long[cmds.size()]);
  }

  private ActionInfo createActionInfoFromAction(SmartAction smartAction,
      long cid) throws IOException {
    ActionStatus status = smartAction.getActionStatus();
    // Replace special character with
    return new ActionInfo(status.getId(),
        cid, smartAction.getName(),
        smartAction.getArguments(),
        StringEscapeUtils.escapeJava(status.getResultStream().toString("UTF-8")),
        StringEscapeUtils.escapeJava(status.getLogStream().toString("UTF-8")),
        status.isSuccessful(), status.getStartTime(),
        status.isFinished(), status.getFinishTime(),
        status.getPercentage());
  }

  private List<ActionInfo> getActionInfoFromCmdlet(long cid) throws IOException {
    ArrayList<ActionInfo> actionInfos = new ArrayList<>();
    Cmdlet cmd = cmdletPool.getCmdlet(cid);
    if (cmd == null) {
      return actionInfos;
    }
    for (SmartAction smartAction : cmd.getActions()) {
      actionInfos.add(createActionInfoFromAction(smartAction, cid));
    }
    return actionInfos;
  }

  public synchronized void batchCmdletStatusUpdate() throws IOException {
    if (cmdletPool == null || metaStore == null) {
      return;
    }
    LOG.info("INFO Number of Caches = {}", statusCache.size());
    LOG.info("INFO Number of Actions = {}", cmdsAll.size());
    if (statusCache.size() == 0) {
      return;
    }
    synchronized (statusCache) {
      ArrayList<ActionInfo> actionInfos = new ArrayList<>();
      for (Iterator<CmdTuple> iter = statusCache.iterator(); iter.hasNext(); ) {
        CmdTuple ct = iter.next();
        actionInfos.addAll(getActionInfoFromCmdlet(ct.cid));
        CmdletInfo cmdinfo = cmdsAll.get(ct.cid);
        cmdletHashSet.remove(cmdinfo.getParameters());
        cmdsAll.remove(ct.cid);
        cmdletPool.deleteCmdlet(ct.cid);
        try {
          metaStore.updateCmdletStatus(ct.cid, ct.rid, ct.state);
        } catch (SQLException e) {
          LOG.error("Batch Cmdlet Status Update error!", e);
          throw new IOException(e);
        }
        iter.remove();
      }
      for (ActionInfo actionInfo : actionInfos) {
        if (actionPool.containsKey(actionInfo.getActionId())) {
          // Remove from actionPool
          actionPool.remove(actionInfo.getActionId());
          Map<String, String> args = actionInfo.getArgs();
          if (args != null && args.size() >= 1) {
            String file = args.get(CmdletDescriptor.HDFS_FILE_PATH);
            if (file != null && fileLock.containsKey(file)) {
              fileLock.remove(file);
            }
          }
        }
      }
      try {
        metaStore.updateActionsTable(actionInfos.toArray(new ActionInfo[actionInfos.size()]));
      } catch (SQLException e) {
        LOG.error("Write CacheObject to DB error!", e);
        throw new IOException(e);
      }
    }
  }

  public class CmdTuple {
    public long cid;
    public long rid;
    public CmdletState state;

    public CmdTuple(long cid, long rid, CmdletState state) {
      this.cid = cid;
      this.rid = rid;
      this.state = state;
    }

    public String toString() {
      return String.format("Rule-%d-cmd-%d", cid, rid);
    }
  }

  private void addToStatusCache(long cid, long rid, CmdletState state) {
    statusCache.add(new CmdTuple(cid, rid, state));
  }

  private void removeFromExecuting(long cid, long rid) {
    Set<Long> cmdsExecuting = cmdsInState.get(CmdletState.EXECUTING.getValue());
    if (cmdsExecuting.size() == 0) {
      return;
    }
    cmdsExecuting.remove(cid);
  }

  public class Callback {

    public void complete(long cid, long rid, CmdletState state) {
      cmdletExecutorThread.interrupt();
      // Update State in CacheObject
      if (cmdsAll.get(cid) == null) {
        LOG.error("Cmdlet is null!");
      }
      LOG.info("Cmdlet {} finished!", cmdsAll.get(cid));
      // Mark cmdletInfo as DONE
      cmdsAll.get(cid).setState(state);
      // Mark cmdlet as DONE
      cmdletPool.setFinished(cid, state);
      LOG.info("Cmdlet {}", state.toString());
      synchronized (statusCache) {
        addToStatusCache(cid, rid, state);
      }
      removeFromExecuting(cid, rid);
    }
  }
}
