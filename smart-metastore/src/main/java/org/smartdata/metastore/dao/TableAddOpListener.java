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

import org.smartdata.metastore.MetaStoreException;
import org.smartdata.metastore.utils.Constants;
import org.smartdata.metastore.utils.TimeGranularity;

import java.util.List;
import java.util.concurrent.ExecutorService;

public abstract class TableAddOpListener {
  AccessCountTableDeque coarseGrainedTableDeque;
  AccessCountTableAggregator tableAggregator;
  ExecutorService executorService;

  TableAddOpListener(
      AccessCountTableDeque deque,
      AccessCountTableAggregator aggregator,
      ExecutorService executorService) {
    this.coarseGrainedTableDeque = deque;
    this.tableAggregator = aggregator;
    this.executorService = executorService;
  }

  public void tableAdded(AccessCountTableDeque fineGrainedTableDeque, AccessCountTable table) {
    final AccessCountTable lastCoarseGrainedTable = lastCoarseGrainedTableFor(table.getEndTime());
    // Todo: optimize contains
    if (!coarseGrainedTableDeque.contains(lastCoarseGrainedTable)) {
      final List<AccessCountTable> tablesToAggregate =
          fineGrainedTableDeque.getTables(
              lastCoarseGrainedTable.getStartTime(), lastCoarseGrainedTable.getEndTime());
      if (tablesToAggregate.size() > 0) {
        this.executorService.submit(
            new Runnable() {
              @Override
              public void run() {
                try {
                  tableAggregator.aggregate(lastCoarseGrainedTable, tablesToAggregate);
                  coarseGrainedTableDeque.add(lastCoarseGrainedTable);
                } catch (MetaStoreException e) {
                  e.printStackTrace();
                }
              }
            });
      }
    }
  }

  public abstract AccessCountTable lastCoarseGrainedTableFor(Long startTime);

  public static class MinuteTableListener extends TableAddOpListener {
    public MinuteTableListener(
        AccessCountTableDeque deque,
        AccessCountTableAggregator aggregator,
        ExecutorService service) {
      super(deque, aggregator, service);
    }

    @Override
    public AccessCountTable lastCoarseGrainedTableFor(Long endTime) {
      Long lastEnd = endTime - (endTime % Constants.ONE_MINUTE_IN_MILLIS);
      Long lastStart = lastEnd - Constants.ONE_MINUTE_IN_MILLIS;
      return new AccessCountTable(lastStart, lastEnd, TimeGranularity.MINUTE);
    }
  }

  public static class HourTableListener extends TableAddOpListener {
    public HourTableListener(
        AccessCountTableDeque deque,
        AccessCountTableAggregator aggregator,
        ExecutorService service) {
      super(deque, aggregator, service);
    }

    @Override
    public AccessCountTable lastCoarseGrainedTableFor(Long endTime) {
      Long lastEnd = endTime - (endTime % Constants.ONE_HOUR_IN_MILLIS);
      Long lastStart = lastEnd - Constants.ONE_HOUR_IN_MILLIS;
      return new AccessCountTable(lastStart, lastEnd, TimeGranularity.HOUR);
    }
  }

  public static class DayTableListener extends TableAddOpListener {
    public DayTableListener(
        AccessCountTableDeque deque,
        AccessCountTableAggregator aggregator,
        ExecutorService service) {
      super(deque, aggregator, service);
    }

    @Override
    public AccessCountTable lastCoarseGrainedTableFor(Long endTime) {
      Long lastEnd = endTime - (endTime % Constants.ONE_DAY_IN_MILLIS);
      Long lastStart = lastEnd - Constants.ONE_DAY_IN_MILLIS;
      return new AccessCountTable(lastStart, lastEnd, TimeGranularity.DAY);
    }
  }

  // Todo: WeekTableListener, MonthTableListener, YearTableListener
}
