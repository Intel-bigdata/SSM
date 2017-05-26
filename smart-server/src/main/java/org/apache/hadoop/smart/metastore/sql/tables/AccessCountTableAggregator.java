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
package org.apache.hadoop.smart.metastore.sql.tables;

import org.apache.hadoop.smart.metastore.sql.DBAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

public class AccessCountTableAggregator {
  private final DBAdapter adapter;
  public static final Logger LOG =
      LoggerFactory.getLogger(AccessCountTableAggregator.class);

  public AccessCountTableAggregator(DBAdapter adapter) {
    this.adapter = adapter;
  }

  public void aggregate(AccessCountTable destinationTable,
      List<AccessCountTable> tablesToAggregate) throws SQLException {
    if (tablesToAggregate.size() > 0) {
      String aggregateSQ = this.aggregateSQLStatement(destinationTable, tablesToAggregate);
      this.adapter.execute(aggregateSQ);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug(tablesToAggregate.size() + " tables aggregated into " + destinationTable);
      for (AccessCountTable table : tablesToAggregate) {
        LOG.debug("\t" + table);
      }
    }
  }

  protected String aggregateSQLStatement(AccessCountTable destinationTable,
      List<AccessCountTable> tablesToAggregate) {
    StringBuilder statement = new StringBuilder();
    statement.append("CREATE TABLE '" + destinationTable.getTableName() + "' as ");
    statement.append("SELECT " + AccessCountTable.FILE_FIELD + ", SUM(" +
        AccessCountTable.ACCESSCOUNT_FIELD + ") as " +
        AccessCountTable.ACCESSCOUNT_FIELD + " FROM (");
    Iterator<AccessCountTable> tableIterator = tablesToAggregate.iterator();
    while (tableIterator.hasNext()) {
      AccessCountTable table = tableIterator.next();
      if (tableIterator.hasNext()) {
        statement.append("SELECT * FROM " + table.getTableName() + " UNION ALL ");
      } else {
        statement.append("SELECT * FROM " + table.getTableName());
      }
    }
    statement.append(") tmp GROUP BY " + AccessCountTable.FILE_FIELD);
    return statement.toString();
  }
}
