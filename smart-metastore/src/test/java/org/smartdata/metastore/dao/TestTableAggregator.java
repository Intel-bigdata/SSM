/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.smartdata.metastore.dao;

import com.google.common.collect.Lists;
import org.dbunit.Assertion;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.xml.XmlDataSet;
import org.junit.Assert;
import org.junit.Test;
import org.smartdata.metastore.DBTest;
import org.smartdata.metastore.MetaStore;
import org.smartdata.metastore.MetaStoreException;
import org.smartdata.model.FileAccessInfo;
import org.smartdata.model.FileInfo;

import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestTableAggregator extends DBTest {

  private void createTables(IDatabaseConnection connection) throws Exception {
    Statement statement = connection.getConnection().createStatement();
    statement.execute(AccessCountDao.createAccessCountTableSQL("table1"));
    statement.execute(AccessCountDao.createAccessCountTableSQL("table2"));
    statement.execute(AccessCountDao.createAccessCountTableSQL("table3"));
    statement.execute(AccessCountDao.createAccessCountTableSQL("expect"));
    statement.close();
  }

  @Test
  public void testAggregate() throws Exception {
    createTables(databaseTester.getConnection());
    IDataSet dataSet =
        new XmlDataSet(getClass().getClassLoader().getResourceAsStream("accessCountTable.xml"));
    databaseTester.setDataSet(dataSet);
    databaseTester.onSetup();
    MetaStore metaStore = new MetaStore(druidPool);
    prepareFiles(metaStore);

    AccessCountTable result = new AccessCountTable("actual", 0L, 0L, false);
    AccessCountTable table1 = new AccessCountTable("table1", 0L, 0L, false);
    AccessCountTable table2 = new AccessCountTable("table2", 0L, 0L, false);
    AccessCountTable table3 = new AccessCountTable("table3", 0L, 0L, false);
    metaStore.aggregateTables(result, Lists.newArrayList(table1, table2, table3));

    ITable actual = databaseTester.getConnection().createTable(result.getTableName());
    ITable expect = databaseTester.getDataSet().getTable("expect");
    Assertion.assertEquals(expect, actual);
  }

  @Test
  public void testGetTopN() throws Exception {
    createTables(databaseTester.getConnection());
    IDataSet dataSet =
        new XmlDataSet(getClass().getClassLoader().getResourceAsStream("accessCountTable.xml"));
    databaseTester.setDataSet(dataSet);
    databaseTester.onSetup();
    MetaStore metaStore = new MetaStore(druidPool);
    prepareFiles(metaStore);

    AccessCountTable table1 = new AccessCountTable("table1", 0L, 0L, false);
    AccessCountTable table2 = new AccessCountTable("table2", 0L, 0L, false);
    AccessCountTable table3 = new AccessCountTable("table3", 0L, 0L, false);

    List<FileAccessInfo> accessInfos =
        metaStore.getHotFiles(Arrays.asList(table1, table2, table3), 1);
    Assert.assertTrue(accessInfos.size() == 1);
    FileAccessInfo expected1 = new FileAccessInfo(103L, "/file3", 7);
    Assert.assertTrue(accessInfos.get(0).equals(expected1));

    List<FileAccessInfo> accessInfos2 =
        metaStore.getHotFiles(Arrays.asList(table1, table2, table3), 2);
    List<FileAccessInfo> expected2 =
        Arrays.asList(expected1, new FileAccessInfo(102L, "/file2", 6));
    Assert.assertTrue(accessInfos2.size() == expected2.size());
    Assert.assertTrue(accessInfos2.containsAll(expected2));
  }

  private void prepareFiles(MetaStore metaStore) throws MetaStoreException {
    List<FileInfo> statusInternals = new ArrayList<>();
    for (int id = 1; id < 6; id++) {
      statusInternals.add(
          new FileInfo(
              "/file" + id,
              id + 100,
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
}
