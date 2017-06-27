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
import org.apache.hadoop.util.Time;

import java.io.PrintStream;

/**
 * Smart action status base.
 */
public class ActionStatus {
  private long id;
  private long startTime;
  private Boolean finished;
  private long finishTime;
  private Boolean successful;
  private ByteArrayOutputStream resultOs;
  private PrintStream psResultOs;
  private ByteArrayOutputStream logOs;
  private PrintStream psLogOs;

  public void init() {
    finished = false;
    startTime = Time.now();
    successful = false;
    resultOs = new ByteArrayOutputStream(64 * 1024);
    psResultOs = new PrintStream(resultOs, false);
    logOs = new ByteArrayOutputStream(64 * 1024);
    psLogOs = new PrintStream(logOs, false);
  }

  public ActionStatus() {
    init();
  }

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public Boolean isFinished() {
    return finished;
  }


  public boolean isSuccessful() {
    return successful;
  }

  public void setSuccessful(boolean successful) {
    this.successful = successful;
  }

  public long getRunningTime() {
    if (finished) {
      return finishTime - startTime;
    }
    return Time.now() - startTime;
  }

  public long getFinishTime() {
    if (finished) {
      return finishTime;
    }
    return Time.now();
  }

  public PrintStream getResultPrintStream() {
    return psResultOs;
  }

  public PrintStream getLogPrintStream() {
    return psLogOs;
  }

  public ByteArrayOutputStream getResultStream() {
    return resultOs;
  }

  public ByteArrayOutputStream getLogStream() {
    return logOs;
  }

  public float getPercentage() {
    if (finished) {
      return 1.0f;
    }
    return 0.0f;//todo
  }
}
