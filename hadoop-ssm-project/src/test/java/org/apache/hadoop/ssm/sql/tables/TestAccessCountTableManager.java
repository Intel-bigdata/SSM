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

import org.apache.hadoop.hdfs.protocol.FilesAccessInfo;
import org.apache.hadoop.ssm.sql.DBAdapter;
import org.apache.hadoop.ssm.sql.DBTest;
import org.apache.hadoop.ssm.utils.TimeGranularity;
import org.dbunit.Assertion;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.SortedTable;
import org.dbunit.dataset.xml.XmlDataSet;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;

public class TestAccessCountTableManager extends DBTest {

  @Test
  public void testAccessCountTableManager() {
    DBAdapter adapter = mock(DBAdapter.class);
    AccessCountTableManager manager = new AccessCountTableManager(adapter);
    Long firstDayEnd = 24 * 60 * 60 * 1000L;
    AccessCountTable accessCountTable = new AccessCountTable(firstDayEnd - 5 * 1000,
      firstDayEnd, TimeGranularity.SECOND);
    manager.addTable(accessCountTable);

    Map<TimeGranularity, AccessCountTableDeque> map = manager.getTableDeques();
    AccessCountTableDeque second = map.get(TimeGranularity.SECOND);
    Assert.assertTrue(second.size() == 1);
    Assert.assertEquals(second.peek(), accessCountTable);

    AccessCountTableDeque minute = map.get(TimeGranularity.MINUTE);
    AccessCountTable minuteTable = new AccessCountTable(firstDayEnd - 60 * 1000,
      firstDayEnd, TimeGranularity.MINUTE);
    Assert.assertTrue(minute.size() == 1);
    Assert.assertEquals(minute.peek(), minuteTable);

    AccessCountTableDeque hour = map.get(TimeGranularity.HOUR);
    AccessCountTable hourTable = new AccessCountTable(firstDayEnd - 60 *60 * 1000,
      firstDayEnd, TimeGranularity.HOUR);
    Assert.assertTrue(hour.size() == 1);
    Assert.assertEquals(hour.peek(), hourTable);

    AccessCountTableDeque day = map.get(TimeGranularity.DAY);
    AccessCountTable dayTable = new AccessCountTable(firstDayEnd - 24 * 60 *60 * 1000,
      firstDayEnd, TimeGranularity.DAY);
    Assert.assertTrue(day.size() == 1);
    Assert.assertEquals(day.peek(), dayTable);
  }

  private void createTables(IDatabaseConnection connection) throws Exception {
    Statement statement = connection.getConnection().createStatement();
    statement.execute(AccessCountTable.createTableSQL("expect1"));
    String sql = "CREATE TABLE `files` (" +
      "`path` varchar(4096) NOT NULL," +
      "`fid` bigint(20) NOT NULL )";
    statement.execute(sql);
    statement.close();
  }

  @Test
  public void testAddAccessCountInfo() throws Exception {
    createTables(databaseTester.getConnection());
    IDataSet dataSet = new XmlDataSet(getClass().getClassLoader()
      .getResourceAsStream("files.xml"));
    databaseTester.setDataSet(dataSet);
    databaseTester.onSetup();

    DBAdapter adapter = new DBAdapter(databaseTester.getConnection().getConnection());
    AccessCountTableManager manager = new AccessCountTableManager(adapter);
    FilesAccessInfo accessInfo = new FilesAccessInfo(0L, 5L);
    Map<String, Integer> map = new HashMap<>();
    map.put("file1", 5);
    map.put("file2", 10);
    map.put("file3", 15);
    accessInfo.setAccessCountMap(map);
    manager.addAccessCountInfo(accessInfo);

    AccessCountTable accessCountTable = new AccessCountTable(0L, 5L);
    ITable actual = databaseTester.getConnection()
        .createTable(accessCountTable.getTableName());
    ITable expect = databaseTester.getDataSet().getTable("expect1");
    SortedTable sortedActual = new SortedTable(actual, new String[]{"file_id"});
    sortedActual.setUseComparable(true);
    Assertion.assertEquals(expect, sortedActual);
  }
}
