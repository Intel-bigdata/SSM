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
package org.smartdata.server.cmdlet.executor;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.smartdata.server.cmdlet.Cmdlet;
import org.smartdata.server.cmdlet.message.ActionStatusReport;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

//Todo: 1. make this a interface so that we could have different executor implementation
//      2. add api providing available resource
public class CmdletExecutor {
  private CmdletStatusReporter reporter;
  private Map<Long, Future> runningCmdlets;
  private ListeningExecutorService executorService;

  public CmdletExecutor(CmdletStatusReporter reporter) {
    this.reporter = reporter;
    this.runningCmdlets = new ConcurrentHashMap<>();
    this.executorService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(10));
  }

  public void execute(Cmdlet cmdlet) {
    ListenableFuture<?> future = this.executorService.submit(cmdlet);
    Futures.addCallback(future, new CmdletCallBack(cmdlet), executorService);
    this.runningCmdlets.put(cmdlet.getId(), future);
  }

  public void stop(Long commandId) {
    if (this.runningCmdlets.containsKey(commandId)) {
      this.runningCmdlets.get(commandId).cancel(true);
      this.runningCmdlets.remove(commandId);
    }
  }

  public void shutdown() {
    this.executorService.shutdown();
  }

  public ActionStatusReport getActionStatusReport() {
    return new ActionStatusReport();
  }

  private class CmdletCallBack implements FutureCallback<Object> {
    private final Cmdlet command;

    public CmdletCallBack(Cmdlet command) {
      this.command = command;
    }

    @Override
    public void onSuccess(@Nullable Object result) {
      runningCmdlets.remove(this.command.getId());
    }

    @Override
    public void onFailure(Throwable t) {
      runningCmdlets.remove(this.command.getId());
    }
  }
}
