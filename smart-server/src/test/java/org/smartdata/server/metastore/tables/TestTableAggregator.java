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
package org.smartdata.server.metastore.tables;

import org.dbunit.database.IDatabaseConnection;
import org.junit.Test;
import org.smartdata.server.metastore.DBTest;

import java.sql.Statement;

public class TestTableAggregator extends DBTest {

  private void createTables(IDatabaseConnection connection) throws Exception {
    Statement statement = connection.getConnection().createStatement();
    String sql = "CREATE TABLE `files` (`path` varchar(4096) NOT NULL," +
      "`fid` bigint(20) NOT NULL )";
    statement.execute(sql);
    statement.execute(AccessCountTable.createTableSQL("table1"));
    statement.execute(AccessCountTable.createTableSQL("table2"));
    statement.execute(AccessCountTable.createTableSQL("table3"));
    statement.execute(AccessCountTable.createTableSQL("expect"));
    statement.close();
  }

  @Test
  public void testAggregate() throws Exception {
    // AccessCountTableAggregator aggregator = new AccessCountTableAggregator(null);
    // createTables(databaseTester.getConnection());
    // IDataSet dataSet = new XmlDataSet(getClass().getClassLoader()
    //   .getResourceAsStream("accessCountTable.xml"));
    // databaseTester.setDataSet(dataSet);
    // databaseTester.onSetup();
    //
    // AccessCountTable result = new AccessCountTable("actual", 0L,
    //   0L, TimeGranularity.MINUTE);
    // AccessCountTable table1 = new AccessCountTable("table1", 0L,
    //   0L, TimeGranularity.SECOND);
    // AccessCountTable table2 = new AccessCountTable("table2", 0L,
    //   0L, TimeGranularity.SECOND);
    // AccessCountTable table3 = new AccessCountTable("table3", 0L,
    //   0L, TimeGranularity.SECOND);
    // String aggregateStatement = aggregator.aggregateSQLStatement(result,
    //   Lists.newArrayList(table1, table2, table3));
    // Statement statement = databaseTester.getConnection().getConnection().createStatement();
    // statement.execute(aggregateStatement);
    // statement.close();
    //
    // ITable actual = databaseTester.getConnection().createTable(result.getTableName());
    // ITable expect = databaseTester.getDataSet().getTable("expect");
    // Assertion.assertEquals(expect, actual);
  }

  @Test
  public void testGetTopN() throws Exception {
    // createTables(databaseTester.getConnection());
    // IDataSet dataSet = new XmlDataSet(getClass().getClassLoader()
    //   .getResourceAsStream("accessCountTable.xml"));
    // databaseTester.setDataSet(dataSet);
    // databaseTester.onSetup();
    // MetaStore adapter = new MetaStore(databaseTester.getConnection().getConnection());
    //
    // AccessCountTable table1 = new AccessCountTable("table1", 0L,
    //   0L, TimeGranularity.SECOND);
    // AccessCountTable table2 = new AccessCountTable("table2", 0L,
    //   0L, TimeGranularity.SECOND);
    // AccessCountTable table3 = new AccessCountTable("table3", 0L,
    //   0L, TimeGranularity.SECOND);
    //
    // List<FileAccessInfo> accessInfos = adapter.getHotFiles(Arrays.asList(table1, table2, table3), 1);
    // Assert.assertTrue(accessInfos.size() == 1);
    // FileAccessInfo expected1 = new FileAccessInfo(103L, "file3", 7);
    // Assert.assertTrue(accessInfos.get(0).equals(expected1));
    //
    // List<FileAccessInfo> accessInfos2 =
    //     adapter.getHotFiles(Arrays.asList(table1, table2, table3), 2);
    // List<FileAccessInfo> expected2 = Arrays.asList(expected1, new FileAccessInfo(102L, "file2", 6));
    // Assert.assertTrue(accessInfos2.size() == expected2.size());
    // Assert.assertTrue(accessInfos2.containsAll(expected2));
  }
}