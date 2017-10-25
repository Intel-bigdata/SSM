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
package org.smartdata.metastore.dao;

import com.google.common.annotations.VisibleForTesting;
import org.smartdata.metastore.utils.TimeGranularity;
import org.smartdata.metastore.utils.TimeUtils;

import java.util.Random;

public class AccessCountTable {
  private final String tableName;
  private final Long startTime;
  private final Long endTime;
  private final TimeGranularity granularity;
  private final boolean isEphemeral;

  public AccessCountTable(Long startTime, Long endTime) {
    this(startTime, endTime, false);
  }

  public AccessCountTable(Long startTime, Long endTime, boolean isEphemeral) {
    this(getTableName(startTime, endTime, isEphemeral), startTime, endTime, isEphemeral);
  }

  @VisibleForTesting
  protected AccessCountTable(String name, Long startTime, Long endTime, boolean isEphemeral) {
    this.startTime = startTime;
    this.endTime = endTime;
    this.granularity = TimeUtils.getGranularity(endTime - startTime);
    this.tableName = name;
    this.isEphemeral = isEphemeral;
  }

  public String getTableName() {
    return tableName;
  }

  private static String getTableName(Long startTime, Long endTime, boolean isView) {
    String tableName = "accessCount_" + startTime + "_" + endTime;
    if (isView) {
      tableName += "_view_" + Math.abs(new Random().nextInt());
    }
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
    return other.getStartTime().equals(this.startTime)
        && other.getEndTime().equals(this.endTime)
        && other.getGranularity().equals(this.granularity);
  }

  @Override
  public int hashCode() {
    int result = tableName.hashCode();
    result = 31 * result + startTime.hashCode();
    result = 31 * result + endTime.hashCode();
    result = 31 * result + granularity.hashCode();
    result = 31 * result + (isEphemeral ? 1 : 0);
    return result;
  }

  @Override
  public String toString() {
    return String.format(
        "AccessCountTable %s starts from %s ends with %s and granularity is %s",
        this.tableName, this.startTime, this.endTime, this.granularity);
  }

  public boolean isEphemeral() {
    return isEphemeral;
  }
}
