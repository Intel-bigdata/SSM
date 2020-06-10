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
package org.smartdata.server.cluster;

/**
 * Contains metrics for SSM nodes related with cmdlet execution.
 *
 */
public class NodeCmdletMetrics {
  private NodeInfo nodeInfo;

  private long registTime;
  private int numExecutors;

  private long cmdletsExecuted;
  private int cmdletsInExecution;

  public NodeInfo getNodeInfo() {
    return nodeInfo;
  }

  public void setNodeInfo(NodeInfo nodeInfo) {
    this.nodeInfo = nodeInfo;
  }

  public long getRegistTime() {
    return registTime;
  }

  public void setRegistTime(long registTime) {
    this.registTime = registTime;
  }

  public int getNumExecutors() {
    return numExecutors;
  }

  public void setNumExecutors(int numExecutors) {
    this.numExecutors = numExecutors;
  }

  public long getCmdletsExecuted() {
    return cmdletsExecuted;
  }

  public void setCmdletsExecuted(long cmdletsExecuted) {
    this.cmdletsExecuted = cmdletsExecuted;
  }

  public synchronized void incCmdletsExecuted() {
    cmdletsExecuted++;
  }

  public int getCmdletsInExecution() {
    return cmdletsInExecution;
  }

  public void setCmdletsInExecution(int cmdletsInExecution) {
    this.cmdletsInExecution = cmdletsInExecution;
  }

  public synchronized void incCmdletsInExecution() {
    cmdletsInExecution++;
  }

  public synchronized void finishCmdlet() {
    cmdletsExecuted++;
    if (cmdletsInExecution > 0) { // TODO: restore
      cmdletsInExecution--;
    }
  }
}
