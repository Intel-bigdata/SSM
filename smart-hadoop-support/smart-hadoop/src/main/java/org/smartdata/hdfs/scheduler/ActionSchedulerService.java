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

import org.smartdata.AbstractService;
import org.smartdata.SmartContext;
import org.smartdata.metastore.MetaStore;
import org.smartdata.model.ActionInfo;
import org.smartdata.model.CmdletInfo;
import org.smartdata.model.LaunchAction;
import org.smartdata.model.action.ActionScheduler;
import org.smartdata.model.action.ScheduleResult;
import org.smartdata.protocol.message.LaunchCmdlet;

import java.io.IOException;

public abstract class ActionSchedulerService extends AbstractService implements ActionScheduler {
  private MetaStore metaStore;

  public ActionSchedulerService(SmartContext context, MetaStore metaStore) {
    super(context);
    this.metaStore = metaStore;
  }

  public boolean onSubmit(CmdletInfo cmdletInfo, ActionInfo actionInfo, int actionIndex)
      throws IOException {
    return true;
  }

  public ScheduleResult onSchedule(CmdletInfo cmdletInfo, ActionInfo actionInfo,
      LaunchCmdlet cmdlet, LaunchAction action, int actionIndex) {
    return ScheduleResult.SUCCESS;
  }

  public void postSchedule(CmdletInfo cmdletInfo, ActionInfo actionInfo, int actionIndex,
      ScheduleResult result) {
  }

  public void onPreDispatch(LaunchCmdlet cmdlet, LaunchAction action, int actionIndex) {
  }

  public void onActionFinished(CmdletInfo cmdletInfo, ActionInfo actionInfo, int actionIndex) {
  }

  public boolean isSuccessfulBySpeculation(ActionInfo actionInfo) {
    return false;
  }
}
