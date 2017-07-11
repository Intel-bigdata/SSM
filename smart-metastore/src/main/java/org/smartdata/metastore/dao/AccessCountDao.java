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

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;
import org.springframework.jdbc.support.rowset.SqlRowSet;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class AccessCountDao {
  private DataSource dataSource;
  public final static String FILE_FIELD = "fid";
  public final static String ACCESSCOUNT_FIELD = "count";

  public void setDataSource(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public AccessCountDao(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public void insert(AccessCountTable accessCountTable) {
    SimpleJdbcInsert simpleJdbcInsert = new SimpleJdbcInsert(dataSource);
    simpleJdbcInsert.setTableName("access_count_tables");
    simpleJdbcInsert.execute(toMap(accessCountTable));
  }

  public void insert(AccessCountTable[] accessCountTables) {
    for (AccessCountTable accessCountTable : accessCountTables) {
      insert(accessCountTable);
    }
  }

  public void delete(Long startTime, Long endTime) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    final String sql =
        String.format(
        "delete from access_count_tables where start_time >= %s AND end_time <= %s",
            startTime,
            endTime);
    jdbcTemplate.update(sql);
  }

  public static String createTableSQL(String tableName) {
    return String.format(
        "CREATE TABLE %s (%s INTEGER NOT NULL, %s INTEGER NOT NULL)",
        tableName, FILE_FIELD, ACCESSCOUNT_FIELD);
  }

  public String aggregateSQLStatement(AccessCountTable destinationTable
      , List<AccessCountTable> tablesToAggregate) {
    StringBuilder statement = new StringBuilder();
    statement.append("CREATE TABLE " + destinationTable.getTableName() + " as ");
    statement.append("SELECT " + AccessCountDao.FILE_FIELD + ", SUM(" +
        AccessCountDao.ACCESSCOUNT_FIELD + ") as " +
        AccessCountDao.ACCESSCOUNT_FIELD + " FROM (");
    statement.append(getUnionStatement(tablesToAggregate));
    statement.append(") tmp GROUP BY " + AccessCountDao.FILE_FIELD);
    return statement.toString();
  }

  public Map<Long, Integer> getHotFiles(List<AccessCountTable> tables, int topNum)
      throws SQLException {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String statement =
        String.format(
            "SELECT %s, SUM(%s) as %s FROM (%s) tmp GROUP BY %s ORDER BY %s DESC LIMIT %s",
            AccessCountDao.FILE_FIELD,
            AccessCountDao.ACCESSCOUNT_FIELD,
            AccessCountDao.ACCESSCOUNT_FIELD,
            getUnionStatement(tables),
            AccessCountDao.FILE_FIELD,
            AccessCountDao.ACCESSCOUNT_FIELD,
            topNum);
    SqlRowSet sqlRowSet = jdbcTemplate.queryForRowSet(statement);
    Map<Long, Integer> accessCounts = new HashMap<>();
    while (sqlRowSet.next()) {
      accessCounts.put(
          sqlRowSet.getLong(AccessCountDao.FILE_FIELD),
          sqlRowSet.getInt(AccessCountDao.ACCESSCOUNT_FIELD));
    }
    return accessCounts;
  }

  private String getUnionStatement(List<AccessCountTable> tables) {
    StringBuilder union = new StringBuilder();
    Iterator<AccessCountTable> tableIterator = tables.iterator();
    while (tableIterator.hasNext()) {
      AccessCountTable table = tableIterator.next();
      if (tableIterator.hasNext()) {
        union.append("SELECT * FROM " + table.getTableName() + " UNION ALL ");
      } else {
        union.append("SELECT * FROM " + table.getTableName());
      }
    }
    return union.toString();
  }

  public void createProportionView(AccessCountTable dest, AccessCountTable source)
      throws SQLException {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    double percentage =
        ((double) dest.getEndTime() - dest.getStartTime())
            / (source.getEndTime() - source.getStartTime());
    String sql =
        String.format(
            "CREATE VIEW %s AS SELECT %s, FLOOR(%s.%s * %s) AS %s FROM %s",
            dest.getTableName(),
            AccessCountDao.FILE_FIELD,
            source.getTableName(),
            AccessCountDao.ACCESSCOUNT_FIELD,
            percentage,
            AccessCountDao.ACCESSCOUNT_FIELD,
            source.getTableName());
    jdbcTemplate.execute(sql);
  }

  private Map<String, Object> toMap(AccessCountTable accessCountTable) {
    Map<String, Object> parameters = new HashMap<>();
    parameters.put("table_name", accessCountTable.getTableName());
    parameters.put("start_time", accessCountTable.getStartTime());
    parameters.put("end_time", accessCountTable.getEndTime());
    return parameters;
  }
}
