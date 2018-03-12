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
package org.smartdata.action;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.SmartContext;
import org.smartdata.protocol.message.ActionStatus;

import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.Map;

/**
 * Smart action, the base class. All actions should inherit this. All actions
 * should be able to run in a cmdlet line or web console. User defined actions
 * are also meant to extend this.
 */
public abstract class SmartAction {
  static final Logger LOG = LoggerFactory.getLogger(SmartAction.class);

  private long actionId;
  private Map<String, String> actionArgs;
  private SmartContext context;
  private ByteArrayOutputStream resultOs;
  private PrintStream psResultOs;
  private ByteArrayOutputStream logOs;
  private PrintStream psLogOs;
  private volatile boolean successful;
  protected String name;
  private long startTime;
  private long finishTime;
  private Throwable throwable;
  private boolean finished;

  public SmartAction() {
    this.successful = false;
    //Todo: extract the print stream out of this class
    this.resultOs = new ByteArrayOutputStream(64 * 1024);
    this.psResultOs = new PrintStream(resultOs, false);
    this.logOs = new ByteArrayOutputStream(64 * 1024);
    this.psLogOs = new PrintStream(logOs, false);
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public SmartContext getContext() {
    return context;
  }

  public void setContext(SmartContext context) {
    this.context = context;
  }

  /**
   * Used to initialize the action.
   *
   * @param args Action specific
   */
  public void init(Map<String, String> args) {
    this.actionArgs = args;
  }

  /**
   * Get action arguments.
   *
   * @return
   */
  public Map<String, String> getArguments() {
    return actionArgs;
  }

  public void setArguments(Map<String, String> args) {
    actionArgs = args;
  }

  public long getActionId() {
    return actionId;
  }

  public void setActionId(long actionId) {
    this.actionId = actionId;
  }

  protected abstract void execute() throws Exception;

  public final void run() {
    try {
      setStartTime();
      execute();
      successful = true;
    } catch (Throwable t) {
      LOG.error("SmartAction execute error ", t);
      setThrowable(t);
      appendLog(ExceptionUtils.getFullStackTrace(t));
    } finally {
      setFinishTime();
      finished = true;
      stop();
    }
  }

  private void setStartTime() {
    this.startTime = System.currentTimeMillis();
  }

  private void setThrowable(Throwable t) {
    this.throwable = t;
  }

  private void setFinishTime() {
    this.finishTime = System.currentTimeMillis();
  }

  protected void appendResult(String result) {
    psResultOs.println(result);
  }

  protected void appendLog(String log) {
    psLogOs.println(log);
  }

  public PrintStream getResultOs() {
    return psResultOs;
  }

  public PrintStream getLogOs() {
    return psLogOs;
  }

  public float getProgress() {
    if (successful) {
      return 1.0F;
    }
    return 0.0F;
  }

  public ActionStatus getActionStatus() throws UnsupportedEncodingException {
    return new ActionStatus(
        actionId,
        getProgress(),
        resultOs.toString("UTF-8"),
        logOs.toString("UTF-8"),
        startTime,
        finishTime,
        throwable,
        finished);
  }

  private void stop() {
    psLogOs.close();
    psResultOs.close();
  }

  public boolean isSuccessful() {
    return successful;
  }

  public boolean isFinished() {
    return finished;
  }

  @VisibleForTesting
  public boolean getExpectedAfterRun() throws UnsupportedEncodingException {
    ActionStatus actionStatus = getActionStatus();
    return actionStatus.isFinished() && actionStatus.getThrowable() == null;
  }
}
