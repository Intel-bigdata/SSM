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
package org.smartdata.metastore.tables;

import org.junit.Assert;
import org.junit.Test;
import org.smartdata.metastore.MetaStore;
import org.smartdata.metastore.utils.TimeGranularity;


import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.mockito.Mockito.mock;

public class TestAddTableOpListener {
  MetaStore adapter = mock(MetaStore.class);
  ExecutorService executorService = Executors.newFixedThreadPool(4);
  AccessCountTableAggregator aggregator = new AccessCountTableAggregator(
      mock(MetaStore.class));

  @Test
  public void testMinuteTableListener() throws InterruptedException {
    Long oneSec = 1000L;
    TableEvictor tableEvictor = new CountEvictor(adapter, 10);
    AccessCountTableDeque minuteTableDeque = new AccessCountTableDeque(tableEvictor);
    TableAddOpListener minuteTableListener =
        new TableAddOpListener.MinuteTableListener(minuteTableDeque, aggregator, executorService);
    AccessCountTableDeque secondTableDeque =
        new AccessCountTableDeque(tableEvictor, minuteTableListener);

    AccessCountTable table1 =
        new AccessCountTable(45 * oneSec, 50 * oneSec, TimeGranularity.SECOND);
    AccessCountTable table2 =
        new AccessCountTable(50 * oneSec, 55 * oneSec, TimeGranularity.SECOND);
    AccessCountTable table3 =
        new AccessCountTable(55 * oneSec, 60 * oneSec, TimeGranularity.SECOND);

    secondTableDeque.add(table1);
    Assert.assertTrue(minuteTableDeque.size() == 0);
    secondTableDeque.add(table2);
    Assert.assertTrue(minuteTableDeque.size() == 0);

    secondTableDeque.add(table3);
    Thread.sleep(1000);

    Assert.assertTrue(minuteTableDeque.size() == 1);
    AccessCountTable expected = new AccessCountTable(0L, 60 * oneSec, TimeGranularity.MINUTE);
    Assert.assertEquals(minuteTableDeque.poll(), expected);
  }

  @Test
  public void testHourTableListener() throws InterruptedException {
    Long oneMin = 60 * 1000L;
    TableEvictor tableEvictor = new CountEvictor(adapter, 10);
    AccessCountTableDeque hourTableDeque = new AccessCountTableDeque(tableEvictor);
    TableAddOpListener hourTableListener =
        new TableAddOpListener.HourTableListener(hourTableDeque, aggregator, executorService);
    AccessCountTableDeque minuteTableDeque =
        new AccessCountTableDeque(tableEvictor, hourTableListener);

    AccessCountTable table1 =
        new AccessCountTable(57 * oneMin, 58 * oneMin, TimeGranularity.MINUTE);
    AccessCountTable table2 =
        new AccessCountTable(58 * oneMin, 59 * oneMin, TimeGranularity.MINUTE);
    AccessCountTable table3 =
        new AccessCountTable(59 * oneMin, 60 * oneMin, TimeGranularity.MINUTE);

    minuteTableDeque.add(table1);
    Assert.assertTrue(hourTableDeque.size() == 0);

    minuteTableDeque.add(table2);
    Assert.assertTrue(hourTableDeque.size() == 0);

    minuteTableDeque.add(table3);
    Thread.sleep(1000);

    Assert.assertTrue(hourTableDeque.size() == 1);
    AccessCountTable expected = new AccessCountTable(0L, 60 * oneMin, TimeGranularity.HOUR);
    Assert.assertEquals(hourTableDeque.poll(), expected);
  }

  @Test
  public void testDayTableListener() throws InterruptedException {
    Long oneHour = 60 * 60 * 1000L;
    TableEvictor tableEvictor = new CountEvictor(adapter, 10);
    AccessCountTableDeque dayTableDeque = new AccessCountTableDeque(tableEvictor);
    TableAddOpListener dayTableListener =
        new TableAddOpListener.DayTableListener(dayTableDeque, aggregator, executorService);
    AccessCountTableDeque hourTableDeque =
        new AccessCountTableDeque(tableEvictor, dayTableListener);

    AccessCountTable table1 =
        new AccessCountTable(21 * oneHour, 22 * oneHour, TimeGranularity.HOUR);
    AccessCountTable table2 =
        new AccessCountTable(22 * oneHour, 23 * oneHour, TimeGranularity.HOUR);
    AccessCountTable table3 =
        new AccessCountTable(23 * oneHour, 24 * oneHour, TimeGranularity.HOUR);

    hourTableDeque.add(table1);
    Assert.assertTrue(dayTableDeque.size() == 0);

    hourTableDeque.add(table2);
    Assert.assertTrue(dayTableDeque.size() == 0);

    hourTableDeque.add(table3);
    Thread.sleep(1000);

    Assert.assertTrue(dayTableDeque.size() == 1);
    AccessCountTable today = new AccessCountTable(0L, 24 * oneHour, TimeGranularity.DAY);
    Assert.assertEquals(dayTableDeque.poll(), today);
  }
}
