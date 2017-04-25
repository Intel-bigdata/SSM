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

import java.util.Iterator;
import java.util.List;

public class AccessCountTableAggregator {
  private final String FILE_FIELD = "file_name";
  private final String ACCESSCOUNT_FIELD = "access_count";

  public void aggregate(AccessCountTable destinationTable,
      List<AccessCountTable> tablesToAggregate) {
    if (tablesToAggregate.size() > 0) {
      String sqlStatement = this.aggregateSQLStatement(destinationTable, tablesToAggregate);
      System.out.println(sqlStatement);
    }
  }

  protected String aggregateSQLStatement(AccessCountTable destinationTable,
      List<AccessCountTable> tablesToAggregate) {
    StringBuilder statement = new StringBuilder();
//    statement.append("CREATE TABLE " + destinationTable.getTableName() + " as ");
    statement.append("SELECT " + FILE_FIELD + ", SUM(" + ACCESSCOUNT_FIELD + ") as "
      + ACCESSCOUNT_FIELD + " FROM (");
    Iterator<AccessCountTable> tableIterator = tablesToAggregate.iterator();
    while (tableIterator.hasNext()) {
      AccessCountTable table = tableIterator.next();
      if (tableIterator.hasNext()) {
        statement.append("SELECT * FROM " + table.getTableName() + " UNION ALL ");
      } else {
        statement.append("SELECT * FROM " + table.getTableName());
      }
    }
    statement.append(") tmp GROUP BY " + FILE_FIELD);
    return statement.toString();
  }
}
