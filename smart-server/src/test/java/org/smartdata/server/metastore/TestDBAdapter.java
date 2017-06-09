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
package org.smartdata.server.metastore;

import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.SortedTable;
import org.dbunit.dataset.xml.XmlDataSet;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.smartdata.server.metastore.DBAdapter;
import org.smartdata.server.metastore.tables.AccessCountTable;
import org.smartdata.server.utils.TimeGranularity;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class TestDBAdapter extends DBTest {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

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
  public void testGetFileIds() throws Exception {
    createTables(databaseTester.getConnection());
    IDataSet dataSet = new XmlDataSet(getClass().getClassLoader()
      .getResourceAsStream("files.xml"));
    databaseTester.setDataSet(dataSet);
    databaseTester.onSetup();

    DBAdapter dbAdapter = new DBAdapter(databaseTester.getConnection().getConnection());
    List<String> paths = Arrays.asList("file1", "file2", "file3");
    Map<String, Long> pathToID = dbAdapter.getFileIDs(paths);
    Assert.assertTrue(pathToID.get("file1") == 101);
    Assert.assertTrue(pathToID.get("file2") == 102);
    Assert.assertTrue(pathToID.get("file3") == 103);
  }

  @Test
  public void testCreateProportionView() throws Exception {
    Statement statement = databaseTester.getConnection().getConnection().createStatement();
    createTables(databaseTester.getConnection());
    statement.execute(AccessCountTable.createTableSQL("table1"));
    statement.execute(AccessCountTable.createTableSQL("table2"));
    statement.execute(AccessCountTable.createTableSQL("table3"));
    statement.execute(AccessCountTable.createTableSQL("expect"));
    IDataSet dataSet = new XmlDataSet(getClass().getClassLoader()
      .getResourceAsStream("accessCountTable.xml"));
    databaseTester.setDataSet(dataSet);
    databaseTester.onSetup();

    AccessCountTable table3 = new AccessCountTable("table3",
      0L, 10L, TimeGranularity.SECOND);
    DBAdapter dbAdapter = new DBAdapter(databaseTester.getConnection().getConnection());
    // 50%
    AccessCountTable viewTable = new AccessCountTable(0L, 5L);
    dbAdapter.createProportionView(viewTable, table3);
    ITable actual = databaseTester.getConnection().createTable(viewTable.getTableName());
    ITable expect = databaseTester.getConnection().createTable(table3.getTableName());
    SortedTable sortedActual = new SortedTable(actual, new String[]{"fid"});
    sortedActual.setUseComparable(true);
    Assert.assertTrue(sortedActual.getRowCount() == expect.getRowCount());

    for (int i = 0; i < expect.getRowCount(); i++) {
      Integer actualAC = (Integer) sortedActual.getValue(i, AccessCountTable.ACCESSCOUNT_FIELD);
      Integer expectAC = (Integer) expect.getValue(i, AccessCountTable.ACCESSCOUNT_FIELD);
      Assert.assertTrue(actualAC == expectAC / 2);
    }
  }

  @Test
  public void testDropTable() throws Exception {
    Statement statement = databaseTester.getConnection().getConnection().createStatement();
    statement.execute(AccessCountTable.createTableSQL("table1"));
    ITable actual = databaseTester.getConnection().createTable("table1");
    Assert.assertNotNull(actual);

    DBAdapter dbAdapter = new DBAdapter(databaseTester.getConnection().getConnection());
    dbAdapter.dropTable("table1");

    thrown.expect(SQLException.class);
    databaseTester.getConnection().createTable("table1");
  }
}
