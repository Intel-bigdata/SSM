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

import java.util.List;

public abstract class TableAddOpListener {
  AccessCountTableList coarseGrainedTable;
  AccessCountTableAggregator tableAggregator;

  TableAddOpListener(AccessCountTableList list, AccessCountTableAggregator aggregator) {
    this.coarseGrainedTable = list;
    this.tableAggregator = aggregator;
  }

  public void tableAdded(AccessCountTableList fineGrainedTableList, AccessCountTable table) {
    // Here is a critical part for handling time window like [59s, 61s)
    AccessCountTable lastCoarseGrainedTable = lastCoarseGrainedTableFor(table.getEndTime());
    if (!coarseGrainedTable.contains(lastCoarseGrainedTable)) {
      List<AccessCountTable> tablesToAggregate =
        fineGrainedTableList.getTables(lastCoarseGrainedTable.getStartTime(),
          lastCoarseGrainedTable.getEndTime());
      coarseGrainedTable.add(lastCoarseGrainedTable);
      this.tableAggregator.aggregate(lastCoarseGrainedTable, tablesToAggregate);
    }
  }

  public abstract AccessCountTable lastCoarseGrainedTableFor(Long startTime);

  public static class MinuteTableListener extends TableAddOpListener {
    private static final Long ONE_MINUTE = 60L * 1000;

    public MinuteTableListener(AccessCountTableList list, AccessCountTableAggregator aggregator) {
      super(list, aggregator);
    }

    @Override
    public AccessCountTable lastCoarseGrainedTableFor(Long endTime) {
      Long lastEnd = endTime - (endTime % ONE_MINUTE);
      Long lastStart = lastEnd - ONE_MINUTE;
      return new AccessCountTable(lastStart, lastEnd, TimeGranularity.MINUTE);
    }
  }

  public static class HourTableListener extends TableAddOpListener {
    private static final Long ONE_HOUR = 60 * MinuteTableListener.ONE_MINUTE;

    public HourTableListener(AccessCountTableList list, AccessCountTableAggregator aggregator) {
      super(list, aggregator);
    }

    @Override
    public AccessCountTable lastCoarseGrainedTableFor(Long endTime) {
      Long lastEnd = endTime - (endTime % ONE_HOUR);
      Long lastStart = lastEnd - ONE_HOUR;
      return new AccessCountTable(lastStart, lastEnd, TimeGranularity.HOUR);
    }
  }

  public static class DayTableListener extends TableAddOpListener {
    private static final Long ONE_DAY = 24 * HourTableListener.ONE_HOUR;

    public DayTableListener(AccessCountTableList list, AccessCountTableAggregator aggregator) {
      super(list, aggregator);
    }

    @Override
    public AccessCountTable lastCoarseGrainedTableFor(Long endTime) {
      Long lastEnd = endTime - (endTime % ONE_DAY);
      Long lastStart = lastEnd - ONE_DAY;
      return new AccessCountTable(lastStart, lastEnd, TimeGranularity.DAY);
    }
  }

  // Todo: WeekTableListener, MonthTableListener, YearTableListener
}
