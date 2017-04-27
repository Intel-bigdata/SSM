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

import org.apache.hadoop.ssm.sql.DBAdapter;
import org.apache.hadoop.ssm.utils.TimeGranularity;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Mockito.mock;

public class TestAddTableOpListener {
  AccessCountTableAggregator aggregator = new AccessCountTableAggregator(
      mock(DBAdapter.class));

  @Test
  public void testMinuteTableListener() {
    Long oneSec = 1000L;
    TableEvictor tableEvictor = new CountEvictor(10);
    AccessCountTableDeque minuteTableDeque = new AccessCountTableDeque(tableEvictor);
    TableAddOpListener minuteTableListener =
        new TableAddOpListener.MinuteTableListener(minuteTableDeque, aggregator);
    AccessCountTableDeque secondTableDeque = new AccessCountTableDeque(
        tableEvictor, minuteTableListener);

    AccessCountTable table1 = new AccessCountTable(45 * oneSec, 50 * oneSec, TimeGranularity.SECOND);
    AccessCountTable table2 = new AccessCountTable(50 * oneSec, 55 * oneSec, TimeGranularity.SECOND);
    AccessCountTable table3 = new AccessCountTable(55 * oneSec, 60 * oneSec, TimeGranularity.SECOND);

    secondTableDeque.add(table1);
    Assert.assertTrue(minuteTableDeque.size() == 1);
    AccessCountTable expected1 = new AccessCountTable(-60 * oneSec, 0L, TimeGranularity.MINUTE);
    Assert.assertEquals(minuteTableDeque.peek(), expected1);

    secondTableDeque.add(table2);
    Assert.assertTrue(minuteTableDeque.size() == 1);

    secondTableDeque.add(table3);
    Assert.assertTrue(minuteTableDeque.size() == 2);
    AccessCountTable expected2 = new AccessCountTable(0L, 60 * oneSec, TimeGranularity.MINUTE);
    minuteTableDeque.poll();
    Assert.assertEquals(minuteTableDeque.poll(), expected2);
  }

  @Test
  public void testHourTableListener() {
    Long oneMin = 60 * 1000L;
    TableEvictor tableEvictor = new CountEvictor(10);
    AccessCountTableDeque hourTableDeque = new AccessCountTableDeque(tableEvictor);
    TableAddOpListener hourTableListener =
        new TableAddOpListener.HourTableListener(hourTableDeque, aggregator);
    AccessCountTableDeque minuteTableDeque = new AccessCountTableDeque(
        tableEvictor, hourTableListener);

    AccessCountTable table1 = new AccessCountTable(57 * oneMin, 58 * oneMin, TimeGranularity.MINUTE);
    AccessCountTable table2 = new AccessCountTable(58 * oneMin, 59 * oneMin, TimeGranularity.MINUTE);
    AccessCountTable table3 = new AccessCountTable(59 * oneMin, 60 * oneMin, TimeGranularity.MINUTE);

    minuteTableDeque.add(table1);
    Assert.assertTrue(hourTableDeque.size() == 1);
    AccessCountTable expected1 = new AccessCountTable(-60 * oneMin, 0L, TimeGranularity.HOUR);
    Assert.assertEquals(hourTableDeque.peek(), expected1);

    minuteTableDeque.add(table2);
    Assert.assertTrue(hourTableDeque.size() == 1);

    minuteTableDeque.add(table3);
    Assert.assertTrue(hourTableDeque.size() == 2);
    AccessCountTable expected2 = new AccessCountTable(0L, 60 * oneMin, TimeGranularity.HOUR);
    hourTableDeque.poll();
    Assert.assertEquals(hourTableDeque.poll(), expected2);
  }

  @Test
  public void testDayTableListener() {
    Long oneHour = 60 * 60 * 1000L;
    TableEvictor tableEvictor = new CountEvictor(10);
    AccessCountTableDeque dayTableDeque = new AccessCountTableDeque(tableEvictor);
    TableAddOpListener dayTableListener =
        new TableAddOpListener.DayTableListener(dayTableDeque, aggregator);
    AccessCountTableDeque hourTableDeque = new AccessCountTableDeque(
        tableEvictor, dayTableListener);

    AccessCountTable table1 = new AccessCountTable(21 * oneHour, 22 * oneHour, TimeGranularity.HOUR);
    AccessCountTable table2 = new AccessCountTable(22 * oneHour, 23 * oneHour, TimeGranularity.HOUR);
    AccessCountTable table3 = new AccessCountTable(23 * oneHour, 24 * oneHour, TimeGranularity.HOUR);

    hourTableDeque.add(table1);
    Assert.assertTrue(dayTableDeque.size() == 1);
    AccessCountTable lastDay = new AccessCountTable(-24 * oneHour, 0L, TimeGranularity.DAY);
    Assert.assertEquals(dayTableDeque.peek(), lastDay);

    hourTableDeque.add(table2);
    Assert.assertTrue(dayTableDeque.size() == 1);

    hourTableDeque.add(table3);
    Assert.assertTrue(dayTableDeque.size() == 2);
    AccessCountTable today = new AccessCountTable(0L, 24 * oneHour, TimeGranularity.DAY);
    dayTableDeque.poll();
    Assert.assertEquals(dayTableDeque.poll(), today);
  }
}
