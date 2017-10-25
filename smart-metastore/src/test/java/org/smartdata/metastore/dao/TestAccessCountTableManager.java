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

import org.dbunit.Assertion;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.SortedTable;
import org.dbunit.dataset.xml.XmlDataSet;
import org.junit.Assert;
import org.junit.Test;
import org.smartdata.metastore.DBTest;
import org.smartdata.metastore.MetaStore;
import org.smartdata.metastore.MetaStoreException;
import org.smartdata.metastore.utils.Constants;
import org.smartdata.metastore.utils.TimeGranularity;
import org.smartdata.metrics.FileAccessEvent;
import org.smartdata.model.FileInfo;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;

public class TestAccessCountTableManager extends DBTest {

  @Test
  public void testAccessCountTableManager() throws InterruptedException {
    MetaStore adapter = mock(MetaStore.class);
    AccessCountTableManager manager = new AccessCountTableManager(adapter);
    Long firstDayEnd = 24 * 60 * 60 * 1000L;
    AccessCountTable accessCountTable =
        new AccessCountTable(firstDayEnd - 5 * 1000, firstDayEnd);
    manager.addTable(accessCountTable);

    Thread.sleep(5000);

    Map<TimeGranularity, AccessCountTableDeque> map = manager.getTableDeques();
    AccessCountTableDeque second = map.get(TimeGranularity.SECOND);
    Assert.assertTrue(second.size() == 1);
    Assert.assertEquals(second.peek(), accessCountTable);

    AccessCountTableDeque minute = map.get(TimeGranularity.MINUTE);
    AccessCountTable minuteTable =
        new AccessCountTable(firstDayEnd - 60 * 1000, firstDayEnd);
    Assert.assertTrue(minute.size() == 1);
    Assert.assertEquals(minute.peek(), minuteTable);

    AccessCountTableDeque hour = map.get(TimeGranularity.HOUR);
    AccessCountTable hourTable =
        new AccessCountTable(firstDayEnd - 60 * 60 * 1000, firstDayEnd);
    Assert.assertTrue(hour.size() == 1);
    Assert.assertEquals(hour.peek(), hourTable);

    AccessCountTableDeque day = map.get(TimeGranularity.DAY);
    AccessCountTable dayTable =
        new AccessCountTable(firstDayEnd - 24 * 60 * 60 * 1000, firstDayEnd);
    Assert.assertTrue(day.size() == 1);
    Assert.assertEquals(day.peek(), dayTable);
  }

  private void createTables(Connection connection) throws Exception {
    Statement statement = connection.createStatement();
    statement.execute(AccessCountDao.createAccessCountTableSQL("expect1"));
    statement.execute(AccessCountDao.createAccessCountTableSQL("expect2"));
    statement.execute(AccessCountDao.createAccessCountTableSQL("expect3"));
    statement.close();
  }

  @Test
  public void testAddAccessCountInfo() throws Exception {
    AccessCountTableManager manager = initTestEnvironment();
    List<FileAccessEvent> accessEvents = new ArrayList<>();
    accessEvents.add(new FileAccessEvent("file1", 0));
    accessEvents.add(new FileAccessEvent("file2", 1));
    accessEvents.add(new FileAccessEvent("file2", 2));
    accessEvents.add(new FileAccessEvent("file3", 2));
    accessEvents.add(new FileAccessEvent("file3", 3));
    accessEvents.add(new FileAccessEvent("file3", 4));

    accessEvents.add(new FileAccessEvent("file3", 5000));

    manager.onAccessEventsArrived(accessEvents);
    assertTableEquals(new AccessCountTable(0L, 5000L).getTableName(), "expect1");
  }

  @Test
  public void testAccessFileNotInNamespace() throws Exception {
    AccessCountTableManager manager = initTestEnvironment();
    List<FileAccessEvent> accessEvents = new ArrayList<>();
    accessEvents.add(new FileAccessEvent("file1", 0));
    accessEvents.add(new FileAccessEvent("file2", 1));
    accessEvents.add(new FileAccessEvent("file2", 2));
    accessEvents.add(new FileAccessEvent("file3", 2));
    accessEvents.add(new FileAccessEvent("file3", 3));
    accessEvents.add(new FileAccessEvent("file3", 4));
    accessEvents.add(new FileAccessEvent("file4", 5));

    accessEvents.add(new FileAccessEvent("file3", 5000));
    accessEvents.add(new FileAccessEvent("file3", 10000));

    manager.onAccessEventsArrived(accessEvents);
    assertTableEquals(new AccessCountTable(0L, 5000L).getTableName(), "expect1");
    assertTableEquals(new AccessCountTable(5000L, 10000L).getTableName(), "expect2");

    accessEvents.clear();
    accessEvents.add(new FileAccessEvent("file4", 10001));
    accessEvents.add(new FileAccessEvent("file4", 10002));
    accessEvents.add(new FileAccessEvent("file3", 15000));
    accessEvents.add(new FileAccessEvent("file4", 15001));
    accessEvents.add(new FileAccessEvent("file3", 20000));
    manager.onAccessEventsArrived(accessEvents);
    assertTableEquals(new AccessCountTable(10000L, 15000L).getTableName(), "expect2");
    assertTableEquals(new AccessCountTable(15000L, 20000L).getTableName(), "expect2");

    insertNewFile(new MetaStore(druidPool), "file4", 4L);
    accessEvents.clear();
    accessEvents.add(new FileAccessEvent("file4", 25000));
    manager.onAccessEventsArrived(accessEvents);
    assertTableEquals(new AccessCountTable(20000L, 25000L).getTableName(), "expect3");
  }

  private AccessCountTableManager initTestEnvironment() throws Exception {
    MetaStore metaStore = new MetaStore(druidPool);
    createTables(databaseTester.getConnection().getConnection());
    IDataSet dataSet = new XmlDataSet(getClass().getClassLoader().getResourceAsStream("files.xml"));
    databaseTester.setDataSet(dataSet);
    databaseTester.onSetup();
    prepareFiles(metaStore);
    return new AccessCountTableManager(metaStore);
  }

  private void assertTableEquals(String actualTableName, String expectedDataSet) throws Exception {
    ITable actual = databaseTester.getConnection().createTable(actualTableName);
    ITable expect = databaseTester.getDataSet().getTable(expectedDataSet);
    SortedTable sortedActual = new SortedTable(actual, new String[] {"fid"});
    sortedActual.setUseComparable(true);
    Assertion.assertEquals(expect, sortedActual);
  }

  private void prepareFiles(MetaStore metaStore) throws MetaStoreException {
    List<FileInfo> statusInternals = new ArrayList<>();
    for (int id = 1; id < 4; id++) {
      statusInternals.add(
          new FileInfo("file" + id,
              id,
              123L,
              false,
              (short) 1,
              128 * 1024L,
              123123123L,
              123123120L,
              (short) 1,
              "root",
              "admin",
              (byte) 0));
    }
    metaStore.insertFiles(statusInternals.toArray(new FileInfo[0]));
  }

  private void insertNewFile(MetaStore metaStore, String file, Long fid)
      throws MetaStoreException {
    FileInfo finfo = new FileInfo(file,
        fid,
        123L,
        false,
        (short) 1,
        128 * 1024L,
        123123123L,
        123123120L,
        (short) 1,
        "root",
        "admin",
        (byte) 0);
    metaStore.insertFile(finfo);
  }

  @Test
  public void testGetTables() throws MetaStoreException {
    MetaStore adapter = mock(MetaStore.class);
    TableEvictor tableEvictor = new CountEvictor(adapter, 20);
    Map<TimeGranularity, AccessCountTableDeque> map = new HashMap<>();
    AccessCountTableDeque dayDeque = new AccessCountTableDeque(tableEvictor);
    AccessCountTable firstDay = new AccessCountTable(0L, Constants.ONE_DAY_IN_MILLIS);
    dayDeque.addAndNotifyListener(firstDay);
    map.put(TimeGranularity.DAY, dayDeque);

    AccessCountTableDeque hourDeque = new AccessCountTableDeque(tableEvictor);
    AccessCountTable firstHour =
        new AccessCountTable(23 * Constants.ONE_HOUR_IN_MILLIS, 24 * Constants.ONE_HOUR_IN_MILLIS);
    AccessCountTable secondHour =
        new AccessCountTable(24 * Constants.ONE_HOUR_IN_MILLIS, 25 * Constants.ONE_HOUR_IN_MILLIS);
    hourDeque.addAndNotifyListener(firstHour);
    hourDeque.addAndNotifyListener(secondHour);
    map.put(TimeGranularity.HOUR, hourDeque);

    AccessCountTableDeque minuteDeque = new AccessCountTableDeque(tableEvictor);
    Integer numMins = 25 * 60;
    AccessCountTable firstMin =
        new AccessCountTable(
            (numMins - 1) * Constants.ONE_MINUTE_IN_MILLIS,
            numMins * Constants.ONE_MINUTE_IN_MILLIS);
    AccessCountTable secondMin =
        new AccessCountTable(
            numMins * Constants.ONE_MINUTE_IN_MILLIS,
            (numMins + 1) * Constants.ONE_MINUTE_IN_MILLIS);
    minuteDeque.addAndNotifyListener(firstMin);
    minuteDeque.addAndNotifyListener(secondMin);
    map.put(TimeGranularity.MINUTE, minuteDeque);

    AccessCountTableDeque secondDeque = new AccessCountTableDeque(tableEvictor);
    Integer numSeconds = (25 * 60 + 1) * 60;
    AccessCountTable firstFiveSeconds =
        new AccessCountTable(
            (numSeconds - 5) * Constants.ONE_SECOND_IN_MILLIS,
            numSeconds * Constants.ONE_SECOND_IN_MILLIS);
    AccessCountTable secondFiveSeconds =
        new AccessCountTable(
            numSeconds * Constants.ONE_SECOND_IN_MILLIS,
            (numSeconds + 5) * Constants.ONE_SECOND_IN_MILLIS);
    secondDeque.addAndNotifyListener(firstFiveSeconds);
    secondDeque.addAndNotifyListener(secondFiveSeconds);
    map.put(TimeGranularity.SECOND, secondDeque);

    List<AccessCountTable> firstResult =
        AccessCountTableManager.getTables(
            map, adapter, (numSeconds + 5) * Constants.ONE_SECOND_IN_MILLIS);
    Assert.assertTrue(firstResult.size() == 4);
    Assert.assertEquals(firstResult.get(0), firstDay);
    Assert.assertEquals(firstResult.get(1), secondHour);
    Assert.assertEquals(firstResult.get(2), secondMin);
    Assert.assertEquals(firstResult.get(3), secondFiveSeconds);

    List<AccessCountTable> secondResult =
        AccessCountTableManager.getTables(
            map, adapter, numSeconds * Constants.ONE_SECOND_IN_MILLIS);
    Assert.assertTrue(secondResult.size() == 4);

    AccessCountTable expectDay =
        new AccessCountTable(5 * Constants.ONE_SECOND_IN_MILLIS, Constants.ONE_DAY_IN_MILLIS);
    Assert.assertEquals(expectDay, secondResult.get(0));

    List<AccessCountTable> thirdResult =
        AccessCountTableManager.getTables(
            map, adapter, secondFiveSeconds.getEndTime() - 23 * Constants.ONE_HOUR_IN_MILLIS);
    Assert.assertTrue(thirdResult.size() == 4);
    Assert.assertEquals(thirdResult.get(0), firstHour);

    List<AccessCountTable> fourthResult =
        AccessCountTableManager.getTables(
            map, adapter, secondFiveSeconds.getEndTime() - 24 * Constants.ONE_HOUR_IN_MILLIS);
    Assert.assertTrue(fourthResult.size() == 3);
    Assert.assertEquals(fourthResult.get(0), secondHour);
  }

  @Test
  public void testGetTablesCornerCase() throws MetaStoreException {
    MetaStore adapter = mock(MetaStore.class);
    TableEvictor tableEvictor = new CountEvictor(adapter, 20);
    Map<TimeGranularity, AccessCountTableDeque> map = new HashMap<>();
    AccessCountTableDeque minute = new AccessCountTableDeque(tableEvictor);
    map.put(TimeGranularity.MINUTE, minute);

    AccessCountTableDeque secondDeque = new AccessCountTableDeque(tableEvictor);
    AccessCountTable firstFiveSeconds =
      new AccessCountTable(0L, 5 * Constants.ONE_SECOND_IN_MILLIS);
    AccessCountTable secondFiveSeconds =
      new AccessCountTable(5 * Constants.ONE_SECOND_IN_MILLIS,
        10 * Constants.ONE_SECOND_IN_MILLIS);
    secondDeque.addAndNotifyListener(firstFiveSeconds);
    secondDeque.addAndNotifyListener(secondFiveSeconds);
    map.put(TimeGranularity.SECOND, secondDeque);

    List<AccessCountTable> result = AccessCountTableManager.getTables(map, adapter,
    2 * Constants.ONE_MINUTE_IN_MILLIS);
    Assert.assertTrue(result.size() == 2);
    Assert.assertTrue(result.get(0).equals(firstFiveSeconds));
    Assert.assertTrue(result.get(1).equals(secondFiveSeconds));
  }

  @Test
  public void testGetTablesCornerCase2() throws MetaStoreException {
    MetaStore adapter = mock(MetaStore.class);
    TableEvictor tableEvictor = new CountEvictor(adapter, 20);
    Map<TimeGranularity, AccessCountTableDeque> map = new HashMap<>();
    AccessCountTableDeque minute = new AccessCountTableDeque(tableEvictor);
    AccessCountTable firstMinute =
      new AccessCountTable(0L, Constants.ONE_MINUTE_IN_MILLIS);
    minute.addAndNotifyListener(firstMinute);
    map.put(TimeGranularity.MINUTE, minute);

    AccessCountTableDeque secondDeque = new AccessCountTableDeque(tableEvictor);
    AccessCountTable firstFiveSeconds =
        new AccessCountTable(
            55 * Constants.ONE_SECOND_IN_MILLIS, 60 * Constants.ONE_SECOND_IN_MILLIS);
    AccessCountTable secondFiveSeconds =
      new AccessCountTable(60 * Constants.ONE_SECOND_IN_MILLIS,
        65 * Constants.ONE_SECOND_IN_MILLIS);
    AccessCountTable thirdFiveSeconds =
      new AccessCountTable(110 * Constants.ONE_SECOND_IN_MILLIS,
        115 * Constants.ONE_SECOND_IN_MILLIS);
    secondDeque.addAndNotifyListener(firstFiveSeconds);
    secondDeque.addAndNotifyListener(secondFiveSeconds);
    secondDeque.addAndNotifyListener(thirdFiveSeconds);
    map.put(TimeGranularity.SECOND, secondDeque);

    List<AccessCountTable> result = AccessCountTableManager.getTables(map, adapter,
      Constants.ONE_MINUTE_IN_MILLIS);
    Assert.assertTrue(result.size() == 3);
    Assert.assertTrue(result.get(0).equals(firstFiveSeconds));
    Assert.assertFalse(result.get(0).isEphemeral());
    Assert.assertTrue(result.get(1).equals(secondFiveSeconds));
    Assert.assertTrue(result.get(2).equals(thirdFiveSeconds));
  }
}
