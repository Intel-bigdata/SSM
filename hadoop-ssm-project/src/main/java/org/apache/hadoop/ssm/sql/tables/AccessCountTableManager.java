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
import org.apache.hadoop.hdfs.protocol.FilesAccessInfo;
import org.apache.hadoop.ssm.sql.DBAdapter;
import org.apache.hadoop.ssm.utils.Constants;
import org.apache.hadoop.ssm.utils.TimeGranularity;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class AccessCountTableManager {
  private static final int NUM_DAY_TABLES_TO_KEEP = 30;
  private static final int NUM_HOUR_TABLES_TO_KEEP = 30;
  private static final int NUM_MINUTE_TABLES_TO_KEEP = 30;
  private static final int NUM_SECOND_TABLES_TO_KEEP = 30;

  private DBAdapter dbAdapter;
  private Map<TimeGranularity, AccessCountTableDeque> tableDeques;
  private AccessCountTableDeque secondTableDeque;

  public AccessCountTableManager(DBAdapter adapter) {
    this.dbAdapter = adapter;
    this.tableDeques = new HashMap<>();
    this.initTables();
  }

  private void initTables(){
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

  public void addAccessCountInfo(FilesAccessInfo accessInfo) {
    long start = accessInfo.getStartTime();
    long end = accessInfo.getEndTime();
    long length = end - start;
    Map<String, Long> pathToFileID = dbAdapter.getFileIDs(
        accessInfo.getAccessCountMap().keySet());
    // Todo: span across multiple minutes?
    if (spanAcrossTwoMinutes(start, end)) {
      long spanPoint = end - end % Constants.ONE_MINUTE_IN_MILLIS;
      FilesAccessInfo first = new FilesAccessInfo(start, spanPoint);
      FilesAccessInfo second = new FilesAccessInfo(spanPoint, end);
      Map<String, Integer> firstAccessMap = new HashMap<>();
      Map<String, Integer> secondAccessMap = new HashMap<>();
      for(Map.Entry<String, Integer> entry : accessInfo.getAccessCountMap().entrySet()) {
        Long firstValue = entry.getValue() * (spanPoint - start) / length;
        firstAccessMap.put(entry.getKey(), firstValue.intValue());
        secondAccessMap.put(entry.getKey(), entry.getValue() - firstValue.intValue());
      }
      first.setAccessCountMap(firstAccessMap);
      second.setAccessCountMap(secondAccessMap);
      this.addAccessCountTable(first, pathToFileID);
      this.addAccessCountTable(second, pathToFileID);
    } else {
      this.addAccessCountTable(accessInfo, pathToFileID);
    }
  }

  private void addAccessCountTable(FilesAccessInfo info, Map<String, Long> pathToFileID) {
    AccessCountTable accessCountTable =
        new AccessCountTable(info.getStartTime(), info.getEndTime());
    String createTable = AccessCountTable.createTableSQL(accessCountTable.getTableName());
    String values = info.getAccessCountMap().entrySet().stream().map(entry ->
        "(" + pathToFileID.get(entry.getKey()) + ", " + entry.getValue() + ")")
        .collect(Collectors.joining(","));

    StringBuilder builder = new StringBuilder(createTable + ";");
    builder.append("INSERT INTO ").append(accessCountTable.getTableName()).append("(")
        .append(AccessCountTable.FILE_FIELD).append(", ")
        .append(AccessCountTable.ACCESSCOUNT_FIELD).append(") VALUES ").append(values);
    // Todo: exception
    try {
      this.dbAdapter.execute(builder.toString());
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  @VisibleForTesting
  protected Map<TimeGranularity, AccessCountTableDeque> getTableDeques() {
    return this.tableDeques;
  }

  private boolean spanAcrossTwoMinutes(long first, long second) {
    return first / Constants.ONE_MINUTE_IN_MILLIS !=
        second / Constants.ONE_MINUTE_IN_MILLIS;
  }
}
