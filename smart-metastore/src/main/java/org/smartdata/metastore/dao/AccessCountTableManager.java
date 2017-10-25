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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.metastore.MetaStore;
import org.smartdata.metastore.MetaStoreException;
import org.smartdata.metastore.utils.TimeGranularity;
import org.smartdata.metastore.utils.TimeUtils;
import org.smartdata.metrics.FileAccessEvent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class AccessCountTableManager {
  private static final int NUM_DAY_TABLES_TO_KEEP = 30;
  private static final int NUM_HOUR_TABLES_TO_KEEP = 48;
  private static final int NUM_MINUTE_TABLES_TO_KEEP = 120;
  private static final int NUM_SECOND_TABLES_TO_KEEP = 30;

  private MetaStore metaStore;
  private Map<TimeGranularity, AccessCountTableDeque> tableDeques;
  private AccessCountTableDeque secondTableDeque;
  private AccessEventAggregator accessEventAggregator;
  private ExecutorService executorService;
  public static final Logger LOG =
      LoggerFactory.getLogger(AccessCountTableManager.class);

  public AccessCountTableManager(MetaStore adapter) {
    this(adapter, Executors.newFixedThreadPool(4));
  }

  public AccessCountTableManager(MetaStore adapter, ExecutorService service) {
    this.metaStore = adapter;
    this.tableDeques = new HashMap<>();
    this.executorService = service;
    this.accessEventAggregator = new AccessEventAggregator(adapter, this);
    this.initTables();
  }

  private void initTables() {
    AccessCountTableAggregator aggregator = new AccessCountTableAggregator(metaStore);
    AccessCountTableDeque dayTableDeque =
        new AccessCountTableDeque(new CountEvictor(metaStore, NUM_DAY_TABLES_TO_KEEP));
    TableAddOpListener dayTableListener =
        new TableAddOpListener.DayTableListener(dayTableDeque, aggregator, executorService);

    AccessCountTableDeque hourTableDeque =
        new AccessCountTableDeque(
            new CountEvictor(metaStore, NUM_HOUR_TABLES_TO_KEEP), dayTableListener);
    TableAddOpListener hourTableListener =
        new TableAddOpListener.HourTableListener(hourTableDeque, aggregator, executorService);

    AccessCountTableDeque minuteTableDeque =
        new AccessCountTableDeque(
            new CountEvictor(metaStore, NUM_MINUTE_TABLES_TO_KEEP), hourTableListener);
    TableAddOpListener minuteTableListener =
        new TableAddOpListener.MinuteTableListener(minuteTableDeque, aggregator, executorService);

    this.secondTableDeque =
        new AccessCountTableDeque(
            new CountEvictor(metaStore, NUM_SECOND_TABLES_TO_KEEP), minuteTableListener);
    this.tableDeques.put(TimeGranularity.SECOND, this.secondTableDeque);
    this.tableDeques.put(TimeGranularity.MINUTE, minuteTableDeque);
    this.tableDeques.put(TimeGranularity.HOUR, hourTableDeque);
    this.tableDeques.put(TimeGranularity.DAY, dayTableDeque);
    this.recoverTables();
  }

  private void recoverTables() {
    try {
      List<AccessCountTable> tables = metaStore.getAllSortedTables();
      for (AccessCountTable table : tables) {
        TimeGranularity timeGranularity =
            TimeUtils.getGranularity(table.getEndTime() - table.getStartTime());
        if (tableDeques.containsKey(timeGranularity)) {
          tableDeques.get(timeGranularity).add(table);
        }
      }
    } catch (MetaStoreException e) {
      LOG.error(e.toString());
    }
  }

  public void addTable(AccessCountTable accessCountTable) {
    if (LOG.isDebugEnabled()) {
      LOG.debug(accessCountTable.toString());
    }
    this.secondTableDeque.addAndNotifyListener(accessCountTable);
  }

  public void onAccessEventsArrived(List<FileAccessEvent> accessEvents) {
    this.accessEventAggregator.addAccessEvents(accessEvents);
  }

  public List<AccessCountTable> getTables(long lengthInMillis) throws MetaStoreException {
    return AccessCountTableManager.getTables(this.tableDeques, this.metaStore, lengthInMillis);
  }

  public static List<AccessCountTable> getTables(
      Map<TimeGranularity, AccessCountTableDeque> tableDeques,
      MetaStore metaStore,
      long lengthInMillis)
      throws MetaStoreException {
    if (tableDeques.isEmpty()) {
      return new ArrayList<>();
    }
    AccessCountTableDeque secondTableDeque = tableDeques.get(TimeGranularity.SECOND);
    if (secondTableDeque == null || secondTableDeque.isEmpty()) {
      return new ArrayList<>();
    }
    long now = secondTableDeque.getLast().getEndTime();
    return getTablesDuring(
        tableDeques, metaStore, lengthInMillis, now, TimeUtils.getGranularity(lengthInMillis));
  }

  // Todo: multi-thread issue
  private static List<AccessCountTable> getTablesDuring(
      final Map<TimeGranularity, AccessCountTableDeque> tableDeques,
      MetaStore metaStore,
      final long length,
      final long endTime,
      final TimeGranularity timeGranularity)
      throws MetaStoreException {
    long startTime = endTime - length;
    AccessCountTableDeque tables = tableDeques.get(timeGranularity);
    List<AccessCountTable> results = new ArrayList<>();
    for (Iterator<AccessCountTable> iterator = tables.iterator(); iterator.hasNext(); ) {
      // Here we assume that the tables are all sorted by time.
      AccessCountTable table = iterator.next();
      if (table.getEndTime() > startTime) {
        if (table.getStartTime() >= startTime) {
          results.add(table);
          startTime = table.getEndTime();
        } else if (table.getStartTime() < startTime) {
          // We got a table should be spilt here. But sometimes we will split out an
          // table that already exists, so this situation should be avoided.
          if (!tableExists(tableDeques, startTime, table.getEndTime())) {
            AccessCountTable splitTable = new AccessCountTable(startTime, table.getEndTime(), true);
            metaStore.createProportionTable(splitTable, table);
            results.add(splitTable);
            startTime = table.getEndTime();
          }
        }
      }
    }
    if (startTime != endTime && !timeGranularity.equals(TimeGranularity.SECOND)) {
      TimeGranularity fineGrained = TimeUtils.getFineGarinedGranularity(timeGranularity);
      results.addAll(
          getTablesDuring(tableDeques, metaStore, endTime - startTime, endTime, fineGrained));
    }
    return results;
  }

  private static boolean tableExists(
      final Map<TimeGranularity, AccessCountTableDeque> tableDeques, long start, long end) {
    TimeGranularity granularity = TimeUtils.getGranularity(end - start);
    AccessCountTable fakeTable = new AccessCountTable(start, end);
    return tableDeques.containsKey(granularity) && tableDeques.get(granularity).contains(fakeTable);
  }

  @VisibleForTesting
  Map<TimeGranularity, AccessCountTableDeque> getTableDeques() {
    return this.tableDeques;
  }
}
