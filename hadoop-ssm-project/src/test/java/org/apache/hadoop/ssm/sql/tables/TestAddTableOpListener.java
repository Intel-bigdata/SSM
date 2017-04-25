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

import org.junit.Assert;
import org.junit.Test;

public class TestAddTableOpListener {
  AccessCountTableAggregator aggregator = new AccessCountTableAggregator();

  @Test
  public void testMinuteTableListener() {
    Long oneSec = 1000L;
    AccessCountTableList minuteTableList = new AccessCountTableList();
    TableAddOpListener minuteTableListener =
      new TableAddOpListener.MinuteTableListener(minuteTableList, aggregator);
    AccessCountTableList secondTableList = new AccessCountTableList(minuteTableListener);

    AccessCountTable table1 = new AccessCountTable(45 * oneSec, 50 * oneSec, TimeGranularity.SECOND);
    AccessCountTable table2 = new AccessCountTable(50 * oneSec, 55 * oneSec, TimeGranularity.SECOND);
    AccessCountTable table3 = new AccessCountTable(55 * oneSec, 60 * oneSec, TimeGranularity.SECOND);

    secondTableList.add(table1);
    Assert.assertTrue(minuteTableList.size() == 1);
    AccessCountTable expected1 = new AccessCountTable(-60 * oneSec, 0L, TimeGranularity.MINUTE);
    Assert.assertEquals(minuteTableList.get(0), expected1);

    secondTableList.add(table2);
    Assert.assertTrue(minuteTableList.size() == 1);

    secondTableList.add(table3);
    Assert.assertTrue(minuteTableList.size() == 2);
    AccessCountTable expected2 = new AccessCountTable(0L, 60 * oneSec, TimeGranularity.MINUTE);
    Assert.assertEquals(minuteTableList.get(1), expected2);
  }

  @Test
  public void testHourTableListener() {
    Long oneMin = 60 * 1000L;
    AccessCountTableList hourTableList = new AccessCountTableList();
    TableAddOpListener hourTableListener =
      new TableAddOpListener.HourTableListener(hourTableList, aggregator);
    AccessCountTableList minuteTableList = new AccessCountTableList(hourTableListener);

    AccessCountTable table1 = new AccessCountTable(57 * oneMin, 58 * oneMin, TimeGranularity.MINUTE);
    AccessCountTable table2 = new AccessCountTable(58 * oneMin, 59 * oneMin, TimeGranularity.MINUTE);
    AccessCountTable table3 = new AccessCountTable(59 * oneMin, 60 * oneMin, TimeGranularity.MINUTE);

    minuteTableList.add(table1);
    Assert.assertTrue(hourTableList.size() == 1);
    AccessCountTable expected1 = new AccessCountTable(-60 * oneMin, 0L, TimeGranularity.HOUR);
    Assert.assertEquals(hourTableList.get(0), expected1);

    minuteTableList.add(table2);
    Assert.assertTrue(hourTableList.size() == 1);

    minuteTableList.add(table3);
    Assert.assertTrue(hourTableList.size() == 2);
    AccessCountTable expected2 = new AccessCountTable(0L, 60 * oneMin, TimeGranularity.HOUR);
    Assert.assertEquals(hourTableList.get(1), expected2);
  }

  @Test
  public void testDayTableListener() {
    Long oneHour = 60 * 60 * 1000L;
    AccessCountTableList dayTableList = new AccessCountTableList();
    TableAddOpListener dayTableListener =
      new TableAddOpListener.DayTableListener(dayTableList, aggregator);
    AccessCountTableList hourTableList = new AccessCountTableList(dayTableListener);

    AccessCountTable table1 = new AccessCountTable(21 * oneHour, 22 * oneHour, TimeGranularity.HOUR);
    AccessCountTable table2 = new AccessCountTable(22 * oneHour, 23 * oneHour, TimeGranularity.HOUR);
    AccessCountTable table3 = new AccessCountTable(23 * oneHour, 24 * oneHour, TimeGranularity.HOUR);

    hourTableList.add(table1);
    Assert.assertTrue(dayTableList.size() == 1);
    AccessCountTable lastDay = new AccessCountTable(-24 * oneHour, 0L, TimeGranularity.DAY);
    Assert.assertEquals(dayTableList.get(0), lastDay);

    hourTableList.add(table2);
    Assert.assertTrue(dayTableList.size() == 1);

    hourTableList.add(table3);
    Assert.assertTrue(dayTableList.size() == 2);
    AccessCountTable today = new AccessCountTable(0L, 24 * oneHour, TimeGranularity.DAY);
    Assert.assertEquals(dayTableList.get(1), today);
  }
}
