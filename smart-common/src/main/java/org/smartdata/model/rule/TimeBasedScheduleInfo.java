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
package org.smartdata.model.rule;

public class TimeBasedScheduleInfo {
  public static final long FOR_EVER = Long.MAX_VALUE;
  private long startTime;
  private long endTime;
  private long[] every;
  private long subScheduleTime;

  private long firstCheckTime;

  public TimeBasedScheduleInfo() {
    every = new long[1];
  }

  public TimeBasedScheduleInfo(long startTime, long endTime, long[] every) {
    this.startTime = startTime;
    this.endTime = endTime;
    this.every = every.clone();
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  public void setEvery(long[] every) {
    this.every = every.clone();
  }

  public long getStartTime() {
    return startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public long[] getEvery() {
    return every;
  }

  public long getMininalEvery() {
    if (every.length == 1) {
      return every[0];
    } else {
      return every[0] > every[2] ? every[2] : every[0];
    }
  }

  public long getBaseEvery() {
    return every[0];
  }

  public boolean isOnce() {
    return startTime == endTime && startTime == 0 && every[0] == 0;
  }

  public boolean isOneShot() {
    return startTime == endTime && every[0] == 0;
  }

  public void setSubScheduleTime(long subScheduleTime) {
    this.subScheduleTime = subScheduleTime;
  }

  public long getSubScheduleTime() {
    return subScheduleTime;
  }

  public long getFirstCheckTime() {
    return firstCheckTime;
  }

  public void setFirstCheckTime(long firstCheckTime) {
    this.firstCheckTime = firstCheckTime;
  }

  public boolean isExecutable(long now) {
    return every.length <= 1 || every[0] == 0 || (now - firstCheckTime) % every[0] < every[1] + 50;
  }
}
