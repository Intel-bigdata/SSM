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

import com.google.common.collect.Lists;
import org.dbunit.Assertion;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.xml.XmlDataSet;
import org.junit.Test;
import org.smartdata.server.metastore.DBTest;
import org.smartdata.server.utils.TimeGranularity;

import java.sql.Statement;

public class TestTableAggregator extends DBTest {

  private void createTables(IDatabaseConnection connection) throws Exception {
    Statement statement = connection.getConnection().createStatement();
    statement.execute(AccessCountTable.createTableSQL("table1"));
    statement.execute(AccessCountTable.createTableSQL("table2"));
    statement.execute(AccessCountTable.createTableSQL("table3"));
    statement.execute(AccessCountTable.createTableSQL("expect"));
    statement.close();
  }

  @Test
  public void testAggregate() throws Exception {
    AccessCountTableAggregator aggregator = new AccessCountTableAggregator(null);
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
