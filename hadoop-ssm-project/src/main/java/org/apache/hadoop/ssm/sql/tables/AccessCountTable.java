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
package org.apache.hadoop.ssm.sql.tables;

public class AccessCountTable {
  private String tableName;
  private Long startTime;
  private Long endTime;
  private TimeGranularity granularity;

  public AccessCountTable(Long startTime, Long endTime, TimeGranularity granularity) {
    this.startTime = startTime;
    this.endTime = endTime;
    this.granularity = granularity;
    this.tableName = "accessCount_" + startTime + "_" + endTime;
  }

  public String getTableName() {
    return tableName;
  }

  public Long getStartTime() {
    return startTime;
  }

  public Long getEndTime() {
    return endTime;
  }

  public TimeGranularity getGranularity() {
    return granularity;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }
    if (o == this) {
      return true;
    }
    if (o.getClass() != getClass()) {
      return false;
    }
    AccessCountTable other = (AccessCountTable) o;
    return other.getStartTime().equals(this.startTime) &&
        other.getEndTime().equals(this.endTime) &&
        other.getGranularity().equals(this.granularity);
  }

  @Override
  public String toString() {
    return "AccessCountTable " + this.tableName + " start from " + this.startTime +
      " end with " + this.endTime + " and granularity is " + this.granularity;
  }
}
