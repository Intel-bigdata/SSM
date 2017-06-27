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
package org.smartdata.actions;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.lang.StringEscapeUtils;
import org.smartdata.SmartContext;
import org.smartdata.common.message.ActionFinished;
import org.smartdata.common.message.ActionStarted;
import org.smartdata.common.message.ActionStatus;
import org.smartdata.common.message.StatusReporter;

import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.Map;

/**
 * Smart action, the base class. All actions should inherit this. All actions
 * should be able to run in a cmdlet line or web console. User defined actions
 * are also meant to extend this.
 */
public abstract class SmartAction {
  private StatusReporter statusReporter;
  private long actionId;
  private Map<String, String> actionArgs;
  private SmartContext context;
  private ByteArrayOutputStream resultOs;
  private PrintStream psResultOs;
  private ByteArrayOutputStream logOs;
  private PrintStream psLogOs;
  private volatile boolean successful;
  protected String name;

  public SmartAction() {
    this(null);
  }

  public SmartAction(StatusReporter statusReporter) {
    this.successful = false;
    this.statusReporter = statusReporter;
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

  public void setStatusReporter(StatusReporter statusReporter) {
    this.statusReporter = statusReporter;
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

  final public void run() {
    Exception exception = null;
    try {
      reportStart();
      execute();
      this.successful = true;
    } catch (Exception e) {
      e.printStackTrace();
      exception = e;
    } finally {
      reportFinished(exception);
      this.stop();
    }
  }

  private void reportStart() {
    if (this.statusReporter != null) {
      this.statusReporter.report(new ActionStarted(this.actionId, System.currentTimeMillis()));
    }
  }

  private void reportFinished(Exception exception) {
    if (this.statusReporter != null) {
      try {
        this.statusReporter.report(
            new ActionFinished(
                this.actionId,
                System.currentTimeMillis(),
                StringEscapeUtils.escapeJava(this.resultOs.toString("UTF-8")),
                StringEscapeUtils.escapeJava(this.logOs.toString("UTF-8")),
                exception));
      } catch (IOException e) {
        e.printStackTrace();
        this.statusReporter.report(
            new ActionFinished(this.actionId, System.currentTimeMillis(), exception));
      }
    }
  }

  protected void appendResult(String result) {
    this.psResultOs.println(result);
  }

  protected void appendLog(String log) {
    this.psLogOs.println(log);
  }

  public float getProgress() {
    if (this.successful) {
      return 1.0F;
    }
    return 0.0F;
  }

  public ActionStatus getActionStatus() throws UnsupportedEncodingException {
    return new ActionStatus(this.actionId, getProgress(),
      StringEscapeUtils.escapeJava(this.resultOs.toString("UTF-8")),
      StringEscapeUtils.escapeJava(this.resultOs.toString("UTF-8")));
  }

  private void stop() {
    this.psLogOs.close();
    this.psResultOs.close();
  }

  public boolean isSuccessful() {
    return this.successful;
  }
}
