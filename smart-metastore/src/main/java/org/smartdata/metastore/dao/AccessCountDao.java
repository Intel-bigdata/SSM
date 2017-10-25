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
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;
import org.springframework.jdbc.support.rowset.SqlRowSet;

import javax.sql.DataSource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class AccessCountDao {
  private DataSource dataSource;
  static final String FILE_FIELD = "fid";
  static final String ACCESSCOUNT_FIELD = "count";

  public void setDataSource(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public AccessCountDao(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public void insert(AccessCountTable accessCountTable) {
    SimpleJdbcInsert simpleJdbcInsert = new SimpleJdbcInsert(dataSource);
    simpleJdbcInsert.setTableName("access_count_table");
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
        "DELETE FROM access_count_table WHERE start_time >= %s AND end_time <= %s",
            startTime,
            endTime);
    jdbcTemplate.update(sql);
  }

  public void delete(AccessCountTable table) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    final String sql = "DELETE FROM access_count_table WHERE table_name = ?";
    jdbcTemplate.update(sql, table.getTableName());
  }

  public static String createAccessCountTableSQL(String tableName) {
    return String.format(
        "CREATE TABLE %s (%s INTEGER NOT NULL, %s INTEGER NOT NULL)",
        tableName, FILE_FIELD, ACCESSCOUNT_FIELD);
  }

  public List<AccessCountTable> getAllSortedTables() {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String sql = "SELECT * FROM access_count_table ORDER BY start_time ASC";
    return jdbcTemplate.query(sql, new AccessCountRowMapper());
  }

  public void aggregateTables(
      AccessCountTable destinationTable, List<AccessCountTable> tablesToAggregate) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String create = AccessCountDao.createAccessCountTableSQL(destinationTable.getTableName());
    jdbcTemplate.execute(create);
    String insert =
        String.format(
            "INSERT INTO %s SELECT %s, SUM(%s) AS %s FROM(%s) tmp GROUP BY %s;",
            destinationTable.getTableName(),
            AccessCountDao.FILE_FIELD,
            AccessCountDao.ACCESSCOUNT_FIELD,
            AccessCountDao.ACCESSCOUNT_FIELD,
            getUnionStatement(tablesToAggregate),
            AccessCountDao.FILE_FIELD);
    jdbcTemplate.execute(insert);
  }

  public Map<Long, Integer> getHotFiles(List<AccessCountTable> tables, int topNum)
      throws SQLException {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String statement =
        String.format(
            "SELECT %s, SUM(%s) AS %s FROM (%s) tmp WHERE %s IN (SELECT fid FROM file) "
                + "GROUP BY %s ORDER BY %s DESC LIMIT %s",
            AccessCountDao.FILE_FIELD,
            AccessCountDao.ACCESSCOUNT_FIELD,
            AccessCountDao.ACCESSCOUNT_FIELD,
            getUnionStatement(tables),
            AccessCountDao.FILE_FIELD,
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

  public void createProportionTable(AccessCountTable dest, AccessCountTable source)
      throws SQLException {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    double percentage =
        ((double) dest.getEndTime() - dest.getStartTime())
            / (source.getEndTime() - source.getStartTime());
    jdbcTemplate.execute(AccessCountDao.createAccessCountTableSQL(dest.getTableName()));
    String sql =
        String.format(
            "INSERT INTO %s SELECT %s, ROUND(%s.%s * %s) AS %s FROM %s",
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

  class AccessCountRowMapper implements RowMapper<AccessCountTable> {
    @Override
    public AccessCountTable mapRow(ResultSet resultSet, int i) throws SQLException {
      AccessCountTable accessCountTable = new AccessCountTable(
        resultSet.getLong("start_time"),
        resultSet.getLong("end_time"));
      return accessCountTable;
    }
  }
}
