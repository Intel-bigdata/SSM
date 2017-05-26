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
package org.apache.hadoop.smart.metastore.sql.tables;

import org.apache.hadoop.smart.utils.TimeGranularity;

public class AccessCountTable {
  public final static String FILE_FIELD = "fid";
  public final static String ACCESSCOUNT_FIELD = "count";

  private String tableName;
  private Long startTime;
  private Long endTime;
  private TimeGranularity granularity;

  private boolean isView;

  public AccessCountTable(Long startTime, Long endTime) {
    this(startTime, endTime, TimeGranularity.SECOND);
  }

  public AccessCountTable(Long startTime, Long endTime, TimeGranularity granularity) {
    this("accessCount_" + startTime + "_" + endTime, startTime, endTime, granularity);
  }

  public AccessCountTable(String name, Long startTime, Long endTime, TimeGranularity granularity) {
    this.startTime = startTime;
    this.endTime = endTime;
    this.granularity = granularity;
    this.tableName = name;
    this.isView = false;
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
    return String.format(
        "AccessCountTable %s starts from %s ends with %s and granularity is %s",
        this.tableName, this.startTime, this.endTime, this.granularity);
  }

  public static String createTableSQL(String tableName) {
    return String.format(
        "CREATE TABLE %s (%s INTEGER NOT NULL, %s INTEGER NOT NULL)",
        tableName, FILE_FIELD, ACCESSCOUNT_FIELD);
  }

  public boolean isView() {
    return isView;
  }

  public void setView(boolean view) {
    isView = view;
  }
}
