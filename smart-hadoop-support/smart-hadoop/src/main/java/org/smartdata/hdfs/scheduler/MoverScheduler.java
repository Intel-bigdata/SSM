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
package org.smartdata.hdfs.scheduler;

import com.google.common.util.concurrent.RateLimiter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.SmartContext;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.hdfs.HadoopUtil;
import org.smartdata.hdfs.action.HdfsAction;
import org.smartdata.hdfs.action.MoveFileAction;
import org.smartdata.hdfs.metric.fetcher.DatanodeStorageReportProcTask;
import org.smartdata.hdfs.metric.fetcher.MovePlanMaker;
import org.smartdata.metastore.MetaStore;
import org.smartdata.model.ActionInfo;
import org.smartdata.model.LaunchAction;
import org.smartdata.model.action.FileMovePlan;
import org.smartdata.model.action.ScheduleResult;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class MoverScheduler extends ActionSchedulerService {
  private DFSClient client;
  private MovePlanStatistics statistics;
  private MovePlanMaker planMaker;
  private final URI nnUri;
  private long dnInfoUpdateInterval = 2 * 60 * 1000;
  private ScheduledExecutorService updateService;
  private ScheduledFuture updateServiceFuture;
  private long throttleInMb;
  private RateLimiter rateLimiter = null;

  public static final Logger LOG =
      LoggerFactory.getLogger(MoverScheduler.class);

  public MoverScheduler(SmartContext context, MetaStore metaStore)
      throws IOException {
    super(context, metaStore);
    nnUri = HadoopUtil.getNameNodeUri(getContext().getConf());
    throttleInMb = getContext().getConf()
        .getLong(SmartConfKeys.SMART_ACTION_MOVE_THROTTLE_MB_KEY,
            SmartConfKeys.SMART_ACTION_MOVE_THROTTLE_MB_DEFAULT);
    if (throttleInMb > 0) {
      rateLimiter = RateLimiter.create(throttleInMb);
    }
  }

  public void init() throws IOException {
    client = HadoopUtil.getDFSClient(nnUri, getContext().getConf());
    statistics = new MovePlanStatistics();
    updateService = Executors.newScheduledThreadPool(1);
  }

  /**
   * After start call, all services and public calls should work.
   * @return
   * @throws IOException
   */
  public void start() throws IOException {
    // TODO: Will be removed when MetaStore part finished
    DatanodeStorageReportProcTask task =
        new DatanodeStorageReportProcTask(client, getContext().getConf());
    task.run();
    planMaker = new MovePlanMaker(client, task.getStorages(), task.getNetworkTopology(), statistics);

    updateServiceFuture = updateService.scheduleAtFixedRate(
        new UpdateClusterInfoTask(task),
        dnInfoUpdateInterval, dnInfoUpdateInterval, TimeUnit.MILLISECONDS);
  }

  /**
   * After stop call, all states in database will not be changed anymore.
   * @throws IOException
   */
  public void stop() throws IOException {
    if (updateServiceFuture != null) {
      updateServiceFuture.cancel(false);
    }
  }

  private static final List<String> actions =
      Arrays.asList("allssd", "onessd", "archive", "alldisk", "onedisk", "ramdisk");
  public List<String> getSupportedActions() {
    return actions;
  }

  @Override
  public ScheduleResult onSchedule(ActionInfo actionInfo, LaunchAction action) {
    if (!actions.contains(action.getActionType())) {
      return ScheduleResult.SUCCESS;
    }

    String file = action.getArgs().get(HdfsAction.FILE_PATH);
    if (file == null) {
      actionInfo.appendLog("File path not specified!\n");
      return ScheduleResult.FAIL;
    }

    String policy = null;
    switch (action.getActionType()) {
      case "allssd":
        policy = "ALL_SSD";
        break;
      case "onessd":
        policy = "ONE_SSD";
        break;
      case "archive":
        policy = "COLD";
        break;
      case "alldisk":
        policy = "HOT";
        break;
      case "onedisk":
        policy = "WARM";
        break;
      case "ramdisk":
        policy = "LAZY_PERSIST";
        break;
    }

    try {
      FileMovePlan plan = planMaker.processNamespace(new Path(file), policy);
      if (rateLimiter != null) {
        // Two possible understandings here: file level and replica level
        int len = (int)(plan.getFileLengthToMove() >> 20);
        if (len > 0) {
          if (!rateLimiter.tryAcquire(len)) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Cancel Scheduling action {} due to throttling. {}", actionInfo, plan);
            }
            return ScheduleResult.RETRY;
          }
        }
      }
      plan.setNamenode(nnUri);
      action.getArgs().put(MoveFileAction.MOVE_PLAN, plan.toString());
      return ScheduleResult.SUCCESS;
    } catch (IOException e) {
      actionInfo.appendLogLine(e.getMessage());
      LOG.error("Exception while processing " + action, e);
      return ScheduleResult.FAIL;
    } catch (Throwable t) {
      actionInfo.appendLogLine(t.getMessage());
      LOG.error("Unexpected exception when scheduling move " + policy + " '" + file + "'.", t);
      return ScheduleResult.FAIL;
    }
  }

  @Override
  public boolean onSubmit(ActionInfo actionInfo) throws IOException {
    // check args
    if (actionInfo.getArgs() == null) {
      throw new IOException("No arguments for the action");
    }
    return true;
  }

  private class UpdateClusterInfoTask implements Runnable {
    private DatanodeStorageReportProcTask task;

    public UpdateClusterInfoTask(DatanodeStorageReportProcTask task) {
      this.task = task;
    }

    @Override
    public void run() {
      try {
        task.run();
        planMaker.updateClusterInfo(task.getStorages(), task.getNetworkTopology());
      } catch (Throwable t) {
        LOG.warn("Exception when updating cluster info ", t);
      }
    }
  }
}
