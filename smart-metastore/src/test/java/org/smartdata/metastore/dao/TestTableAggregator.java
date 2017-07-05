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
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSUtil;
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
import org.smartdata.metastore.utils.TimeGranularity;
import org.smartdata.model.FileAccessInfo;
import org.smartdata.model.FileStatusInternal;

import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestTableAggregator extends DBTest {

  private void createTables(IDatabaseConnection connection) throws Exception {
    Statement statement = connection.getConnection().createStatement();
    statement.execute(AccessCountDao.createTableSQL("table1"));
    statement.execute(AccessCountDao.createTableSQL("table2"));
    statement.execute(AccessCountDao.createTableSQL("table3"));
    statement.execute(AccessCountDao.createTableSQL("expect"));
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

    AccessCountTable result = new AccessCountTable("actual", 0L, 0L, TimeGranularity.MINUTE);
    AccessCountTable table1 = new AccessCountTable("table1", 0L, 0L, TimeGranularity.SECOND);
    AccessCountTable table2 = new AccessCountTable("table2", 0L, 0L, TimeGranularity.SECOND);
    AccessCountTable table3 = new AccessCountTable("table3", 0L, 0L, TimeGranularity.SECOND);
    String aggregateStatement =
        metaStore.aggregateSQLStatement(result, Lists.newArrayList(table1, table2, table3));
    Statement statement = databaseTester.getConnection().getConnection().createStatement();
    statement.execute(aggregateStatement);
    statement.close();

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

    AccessCountTable table1 = new AccessCountTable("table1", 0L, 0L, TimeGranularity.SECOND);
    AccessCountTable table2 = new AccessCountTable("table2", 0L, 0L, TimeGranularity.SECOND);
    AccessCountTable table3 = new AccessCountTable("table3", 0L, 0L, TimeGranularity.SECOND);

    List<FileAccessInfo> accessInfos =
        metaStore.getHotFiles(Arrays.asList(table1, table2, table3), 1);
    Assert.assertTrue(accessInfos.size() == 1);
    FileAccessInfo expected1 = new FileAccessInfo(103L, "/file3", 7);
    Assert.assertTrue(accessInfos.get(0).equals(expected1));

    List<FileAccessInfo> accessInfos2 =
        metaStore.getHotFiles(Arrays.asList(table1, table2, table3), 2);
    List<FileAccessInfo> expected2 = Arrays.asList(expected1, new FileAccessInfo(102L, "/file2", 6));
    Assert.assertTrue(accessInfos2.size() == expected2.size());
    Assert.assertTrue(accessInfos2.containsAll(expected2));
  }

  private void prepareFiles(MetaStore metaStore) throws MetaStoreException {
    List<FileStatusInternal> statusInternals = new ArrayList<>();
    for (int id = 1; id < 6; id++) {
      statusInternals.add(
        new FileStatusInternal(
          123L,
          false,
          1,
          128 * 1024L,
          123123123L,
          123123120L,
          FsPermission.getDefault(),
          "root",
          "admin",
          null,
          DFSUtil.string2Bytes("file" + id),
          "/",
          id + 100,
          0,
          null,
          (byte) 0));
    }
    metaStore.insertFiles(statusInternals.toArray(new FileStatusInternal[0]));
  }
}
