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

import org.apache.hadoop.util.Time;

import java.util.UUID;

/**
 * Smart action status base.
 */
public abstract class ActionStatus {
  private UUID id;
  private Boolean finished;
  private long startTime;
  private Boolean successful;
  private long totalDuration;
  private long totalBlocks;
  private long totalSize;

  public void init() {
    finished = false;
    startTime = Time.monotonicNow();
    successful = false;
    totalDuration = 0;
    totalBlocks = 0;
    totalSize = 0;
  }

  public ActionStatus(UUID id) {
    this.id = id;
    init();
  }

  public UUID getId() {
    return id;
  }

  public Boolean isFinished() {
    return finished;
  }

  public void setFinished(boolean finished) {
    this.finished = finished;
  }

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public boolean isSuccessful() {
    return successful;
  }

  public void setSuccessful(boolean successful) {
    this.successful = successful;
  }

  public void setTotalDuration(long totalDuration) {
    this.totalDuration = totalDuration;
  }

  public long getRunningTime() {
    if (totalDuration != 0) {
      return totalDuration;
    }
    return Time.monotonicNow() - startTime;
  }

  public void reset() {
    finished = false;
    startTime = Time.monotonicNow();
    successful = false;
    totalDuration = 0;
  }

  public long getTotalSize() {
    return totalSize;
  }

  public float getPercentage() {
    return 0.0f;//todo
  }
}
