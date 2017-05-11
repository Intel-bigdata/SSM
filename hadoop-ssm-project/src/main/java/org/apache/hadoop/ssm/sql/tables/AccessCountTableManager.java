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
import org.apache.hadoop.hdfs.protocol.FileAccessEvent;
import org.apache.hadoop.ssm.sql.DBAdapter;
import org.apache.hadoop.ssm.utils.TimeGranularity;
import org.apache.hadoop.ssm.utils.TimeUtils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class AccessCountTableManager {
  private static final int NUM_DAY_TABLES_TO_KEEP = 30;
  private static final int NUM_HOUR_TABLES_TO_KEEP = 30;
  private static final int NUM_MINUTE_TABLES_TO_KEEP = 30;
  private static final int NUM_SECOND_TABLES_TO_KEEP = 30;

  private DBAdapter dbAdapter;
  private Map<TimeGranularity, AccessCountTableDeque> tableDeques;
  private AccessCountTableDeque secondTableDeque;
  private AccessEventAggregator accessEventAggregator;

  public AccessCountTableManager(DBAdapter adapter) {
    this.dbAdapter = adapter;
    this.tableDeques = new HashMap<>();
    this.accessEventAggregator = new AccessEventAggregator(adapter, this);
    this.initTables();
  }

  private void initTables() {
    AccessCountTableAggregator aggregator = new AccessCountTableAggregator(dbAdapter);
    AccessCountTableDeque dayTableDeque = new AccessCountTableDeque(
        new CountEvictor(NUM_DAY_TABLES_TO_KEEP));
    TableAddOpListener dayTableListener =
        new TableAddOpListener.DayTableListener(dayTableDeque, aggregator);

    AccessCountTableDeque hourTableDeque = new AccessCountTableDeque(
        new CountEvictor(NUM_HOUR_TABLES_TO_KEEP), dayTableListener);
    TableAddOpListener hourTableListener =
        new TableAddOpListener.HourTableListener(hourTableDeque, aggregator);

    AccessCountTableDeque minuteTableDeque = new AccessCountTableDeque(
      new CountEvictor(NUM_MINUTE_TABLES_TO_KEEP), hourTableListener);
    TableAddOpListener minuteTableListener =
      new TableAddOpListener.MinuteTableListener(minuteTableDeque, aggregator);

    this.secondTableDeque = new AccessCountTableDeque(
      new CountEvictor(NUM_SECOND_TABLES_TO_KEEP), minuteTableListener);
    this.tableDeques.put(TimeGranularity.SECOND, this.secondTableDeque);
    this.tableDeques.put(TimeGranularity.MINUTE, minuteTableDeque);
    this.tableDeques.put(TimeGranularity.HOUR, hourTableDeque);
    this.tableDeques.put(TimeGranularity.DAY, dayTableDeque);
  }

  public void addTable(AccessCountTable accessCountTable) {
    this.secondTableDeque.add(accessCountTable);
  }

  public void onAccessEventsArrived(List<FileAccessEvent> accessEvents) {
    this.accessEventAggregator.addAccessEvents(accessEvents);
  }

  public List<AccessCountTable> getTables(long lengthInMillis) throws SQLException {
    return AccessCountTableManager.getTables(this.tableDeques, this.dbAdapter, lengthInMillis);
  }

  public static List<AccessCountTable> getTables(
      Map<TimeGranularity, AccessCountTableDeque> tableDeques,
      DBAdapter adapter,
      long lengthInMillis)
      throws SQLException {
    if (tableDeques.isEmpty()) {
      return new ArrayList<>();
    }
    AccessCountTableDeque secondTableDeque = tableDeques.get(TimeGranularity.SECOND);
    if (secondTableDeque == null || secondTableDeque.isEmpty()) {
      return new ArrayList<>();
    }
    long now = secondTableDeque.getLast().getEndTime();
    return getTablesDuring(tableDeques, adapter, lengthInMillis, now);
  }

  // Todo: multi-thread issue
  private static List<AccessCountTable> getTablesDuring(
      final Map<TimeGranularity, AccessCountTableDeque> tableDeques,
      DBAdapter adapter,
      final long length,
      final long endTime)
      throws SQLException {
    long startTime = endTime - length;
    TimeGranularity timeGranularity = TimeUtils.getGranularity(length);
    AccessCountTableDeque tables = tableDeques.get(timeGranularity);
    List<AccessCountTable> results = new ArrayList<>();
    for (Iterator<AccessCountTable> iterator = tables.iterator(); iterator.hasNext(); ) {
      // Here we assume that the tables are all sorted by time.
      AccessCountTable table = iterator.next();
      if (table.getEndTime() > startTime) {
        if (table.getStartTime() == startTime) {
          results.add(table);
        } else if (table.getStartTime() < startTime) {
          // We got a table should be spilt here.
          AccessCountTable splitTable = new AccessCountTable(startTime, table.getEndTime());
          splitTable.setView(true);
          adapter.createProportionView(splitTable, table);
          results.add(splitTable);
        }
        startTime = table.getEndTime();
      }
    }
    if (startTime != endTime && !timeGranularity.equals(TimeGranularity.SECOND)) {
      results.addAll(getTablesDuring(tableDeques, adapter, endTime - startTime, endTime));
    }
    return results;
  }

  @VisibleForTesting
  protected Map<TimeGranularity, AccessCountTableDeque> getTableDeques() {
    return this.tableDeques;
  }
}
