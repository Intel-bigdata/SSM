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
package org.smartdata.server.engine.rule;


import org.smartdata.rule.ScheduleInfo;
import org.smartdata.model.rule.TimeBasedScheduleInfo;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Schedule the execution.
 */
public class ExecutorScheduler {
  private ScheduledExecutorService service;

  public ExecutorScheduler(int numThreads) {
    service = Executors.newScheduledThreadPool(numThreads);
  }

  public void addPeriodicityTask(RuleExecutor re) {
    TimeBasedScheduleInfo si = re.getTranslateResult().getTbScheduleInfo();
    long now = System.currentTimeMillis();
    service.scheduleAtFixedRate(re, si.getStartTime() - now,
        si.getEvery(), TimeUnit.MILLISECONDS);
  }

  public void addPeriodicityTask(ScheduleInfo schInfo, Runnable work) {
    long now = System.currentTimeMillis();
    service.scheduleAtFixedRate(work, schInfo.getStartTime() - now,
        schInfo.getRate(), TimeUnit.MILLISECONDS);
  }

  // TODO: to be defined
  public void addEventTask() {
  }

  public void shutdown() {
    try {
      service.shutdown();
      if (!service.awaitTermination(3000, TimeUnit.MILLISECONDS)) {
        service.shutdownNow();
      }
    } catch (InterruptedException e) {
      service.shutdownNow();
    }
  }


  /**
   * This will be used for extension: a full event based scheduler.
   */
  private class EventGenTask implements Runnable {
    private long id;
    private ScheduleInfo scheduleInfo;
    private int triggered;

    public EventGenTask(long id) {
      this.id = id;
    }

    @Override
    public void run() {
      triggered++;
      if (triggered <= scheduleInfo.getRounds()) {
      } else {
        exitSchduler();
      }
    }

    private void exitSchduler() {
      String[] temp = new String[1];
      temp[1] += "The exception is created deliberately";
    }
  }
}
