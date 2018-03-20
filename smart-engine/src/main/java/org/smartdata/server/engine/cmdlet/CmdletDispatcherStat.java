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
package org.smartdata.server.engine.cmdlet;

public class CmdletDispatcherStat {
  private int statRound = 0;
  private int statFail = 0;
  private int statDispatched = 0;
  private int statNoMoreCmdlet = 0;
  private int statFull = 0;

  public CmdletDispatcherStat() {
  }

  public CmdletDispatcherStat(int statRound, int statFail, int statDispatched,
      int statNoMoreCmdlet, int statFull) {
    this.statRound = statRound;
    this.statFail = statFail;
    this.statDispatched = statDispatched;
    this.statNoMoreCmdlet = statNoMoreCmdlet;
    this.statFull = statFull;
  }

  public int getStatRound() {
    return statRound;
  }

  public void setStatRound(int statRound) {
    this.statRound = statRound;
  }

  public void addStatRound(int val) {
    this.statRound += val;
  }

  public int getStatFail() {
    return statFail;
  }

  public void setStatFail(int statFail) {
    this.statFail = statFail;
  }

  public void addStatFail(int val) {
    this.statFail += val;
  }

  public int getStatDispatched() {
    return statDispatched;
  }

  public void setStatDispatched(int statDispatched) {
    this.statDispatched = statDispatched;
  }

  public void addStatDispatched(int val) {
    this.statDispatched += val;
  }

  public int getStatNoMoreCmdlet() {
    return statNoMoreCmdlet;
  }

  public void setStatNoMoreCmdlet(int statNoMoreCmdlet) {
    this.statNoMoreCmdlet = statNoMoreCmdlet;
  }

  public void addStatNoMoreCmdlet(int val) {
    this.statNoMoreCmdlet += val;
  }

  public int getStatFull() {
    return statFull;
  }

  public void setStatFull(int statFull) {
    this.statFull = statFull;
  }

  public void addStatFull(int val) {
    this.statFull += val;
  }

  public void add(CmdletDispatcherStat stat) {
    this.statRound += stat.statRound;
    this.statFail += stat.statFail;
    this.statDispatched += stat.statDispatched;
    this.statFull += stat.statFull;
    this.statNoMoreCmdlet += stat.statNoMoreCmdlet;
  }
}
