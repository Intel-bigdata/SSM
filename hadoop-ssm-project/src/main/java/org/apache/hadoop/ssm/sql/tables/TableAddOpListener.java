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

import org.apache.hadoop.ssm.utils.Constants;
import org.apache.hadoop.ssm.utils.TimeGranularity;

import java.sql.SQLException;
import java.util.List;

public abstract class TableAddOpListener {
  AccessCountTableDeque coarseGrainedTable;
  AccessCountTableAggregator tableAggregator;

  TableAddOpListener(AccessCountTableDeque deque, AccessCountTableAggregator aggregator) {
    this.coarseGrainedTable = deque;
    this.tableAggregator = aggregator;
  }

  public void tableAdded(AccessCountTableDeque fineGrainedTableDeque, AccessCountTable table) {
    // Here is a critical part for handling time window like [59s, 61s)
    AccessCountTable lastCoarseGrainedTable = lastCoarseGrainedTableFor(table.getEndTime());
    // Todo: optimize contains
    if (!coarseGrainedTable.contains(lastCoarseGrainedTable)) {
      List<AccessCountTable> tablesToAggregate =
        fineGrainedTableDeque.getTables(lastCoarseGrainedTable.getStartTime(),
          lastCoarseGrainedTable.getEndTime());
      if (tablesToAggregate.size() > 0) {
        coarseGrainedTable.add(lastCoarseGrainedTable);
        //Todo: exception
        try {
          this.tableAggregator.aggregate(lastCoarseGrainedTable, tablesToAggregate);
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
    }
  }

  public abstract AccessCountTable lastCoarseGrainedTableFor(Long startTime);

  public static class MinuteTableListener extends TableAddOpListener {
    public MinuteTableListener(AccessCountTableDeque deque, AccessCountTableAggregator aggregator) {
      super(deque, aggregator);
    }

    @Override
    public AccessCountTable lastCoarseGrainedTableFor(Long endTime) {
      Long lastEnd = endTime - (endTime % Constants.ONE_MINUTE_IN_MILLIS);
      Long lastStart = lastEnd - Constants.ONE_MINUTE_IN_MILLIS;
      return new AccessCountTable(lastStart, lastEnd, TimeGranularity.MINUTE);
    }
  }

  public static class HourTableListener extends TableAddOpListener {
    public HourTableListener(AccessCountTableDeque deque, AccessCountTableAggregator aggregator) {
      super(deque, aggregator);
    }

    @Override
    public AccessCountTable lastCoarseGrainedTableFor(Long endTime) {
      Long lastEnd = endTime - (endTime % Constants.ONE_HOUR_IN_MILLIS);
      Long lastStart = lastEnd - Constants.ONE_HOUR_IN_MILLIS;
      return new AccessCountTable(lastStart, lastEnd, TimeGranularity.HOUR);
    }
  }

  public static class DayTableListener extends TableAddOpListener {
    public DayTableListener(AccessCountTableDeque deque, AccessCountTableAggregator aggregator) {
      super(deque, aggregator);
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
