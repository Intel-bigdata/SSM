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

import com.google.common.collect.Lists;
import org.apache.hadoop.ssm.utils.TimeGranularity;
import org.dbunit.Assertion;
import org.dbunit.IDatabaseTester;
import org.dbunit.JdbcDatabaseTester;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.xml.XmlDataSet;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.sql.Statement;

public class TestTableAggregator {
  private static final String DB_PATH = "/tmp/test.db";

  @Before
  public void setUp() {
    File db = new File(DB_PATH);
    if (db.exists()) {
      db.delete();
    }
  }

  private void createTables(IDatabaseConnection connection) throws Exception {
    Statement statement = connection.getConnection().createStatement();
    statement.execute("CREATE TABLE table1 (\n" +
      "  file_name varchar(20) NOT NULL,\n" +
      "  access_count int(11) NOT NULL\n" +
      ")");
    statement.execute("CREATE TABLE table2 (\n" +
      "  file_name varchar(20) NOT NULL,\n" +
      "  access_count int(11) NOT NULL\n" +
      ")");
    statement.execute("CREATE TABLE table3 (\n" +
      "  file_name varchar(20) NOT NULL,\n" +
      "  access_count int(11) NOT NULL\n" +
      ")");
    statement.execute("CREATE TABLE expect (\n" +
      "  file_name varchar(20) NOT NULL,\n" +
      "  access_count int(11) NOT NULL\n" +
      ")");
    statement.close();
  }

  @Test
  public void testAggregate() throws Exception {
    AccessCountTableAggregator aggregator = new AccessCountTableAggregator(null);
    IDatabaseTester databaseTester = new JdbcDatabaseTester("org.sqlite.JDBC",
      "jdbc:sqlite:" + DB_PATH);
    createTables(databaseTester.getConnection());
    IDataSet dataSet = new XmlDataSet(getClass().getClassLoader()
      .getResourceAsStream("accessCountTable.xml"));
    databaseTester.setDataSet(dataSet);
    databaseTester.onSetup();

    AccessCountTable result = new AccessCountTable("actual", 0L,
      0L, TimeGranularity.MINUTE);
    AccessCountTable table1 = new AccessCountTable("table1", 0L,
      0L, TimeGranularity.SECOND);
    AccessCountTable table2 = new AccessCountTable("table2", 0L,
      0L, TimeGranularity.SECOND);
    AccessCountTable table3 = new AccessCountTable("table3", 0L,
      0L, TimeGranularity.SECOND);
    String aggregateStatement = aggregator.aggregateSQLStatement(result,
        Lists.newArrayList(table1, table2, table3));
    Statement statement = databaseTester.getConnection().getConnection().createStatement();
    statement.execute(aggregateStatement);
    statement.close();

    ITable actual = databaseTester.getConnection().createTable(result.getTableName());
    ITable expect = databaseTester.getDataSet().getTable("expect");
    Assertion.assertEquals(expect, actual);
  }
}
