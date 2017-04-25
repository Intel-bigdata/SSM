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

import com.google.common.annotations.VisibleForTesting;

import java.util.HashMap;
import java.util.Map;

public class AccessCountTableManager {
  private Map<TimeGranularity, AccessCountTableList> tableLists;
  private AccessCountTableList secondTableList;

  public AccessCountTableManager() {
    AccessCountTableAggregator aggregator = new AccessCountTableAggregator();
    AccessCountTableList dayTableList = new AccessCountTableList();
    TableAddOpListener dayTableListener =
      new TableAddOpListener.DayTableListener(dayTableList, aggregator);
    AccessCountTableList hourTableList = new AccessCountTableList(dayTableListener);
    TableAddOpListener hourTableListener =
      new TableAddOpListener.HourTableListener(hourTableList, aggregator);
    AccessCountTableList minuteTableList = new AccessCountTableList(hourTableListener);
    TableAddOpListener minuteTableListener =
      new TableAddOpListener.MinuteTableListener(minuteTableList, aggregator);
    this.secondTableList = new AccessCountTableList(minuteTableListener);
    this.tableLists = new HashMap<>();
    this.tableLists.put(TimeGranularity.SECOND, this.secondTableList);
    this.tableLists.put(TimeGranularity.MINUTE, minuteTableList);
    this.tableLists.put(TimeGranularity.HOUR, hourTableList);
    this.tableLists.put(TimeGranularity.DAY, dayTableList);
  }

  public void addSecondTable(AccessCountTable accessCountTable) {
    assert accessCountTable.getGranularity().equals(TimeGranularity.SECOND);
    this.secondTableList.add(accessCountTable);
  }

  @VisibleForTesting
  protected Map<TimeGranularity, AccessCountTableList> getTableLists() {
    return this.tableLists;
  }
}
