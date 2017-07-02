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
        "delete from access_count_tables where start_time >= ? AND end_time <= ?",
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
    Iterator<AccessCountTable> tableIterator = tablesToAggregate.iterator();
    while (tableIterator.hasNext()) {
      AccessCountTable table = tableIterator.next();
      if (tableIterator.hasNext()) {
        statement.append("SELECT * FROM " + table.getTableName() + " UNION ALL ");
      } else {
        statement.append("SELECT * FROM " + table.getTableName());
      }
    }
    statement.append(") tmp GROUP BY " + AccessCountDao.FILE_FIELD);
    return statement.toString();
  }


  /*public Map<Long, Integer> getHotFiles(long startTime, long endTime,
                                           String countFilter) throws SQLException {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    Map<Long, Integer> ret = new HashMap<>();
    String sqlGetTableNames = String.format(
        "SELECT table_name FROM access_count_tables "
            + "WHERE start_time >= %s AND end_time <= %s",
        startTime,
        endTime);
    List<AccessCountTable> list = jdbcTemplate.query(sqlGetTableNames, new AccessCountRowMapper());
    if(list == null) {
      return null;
    }
    String sqlPrefix = "SELECT fid, SUM(count) AS count FROM (";
    String sqlUnion = "SELECT fid, count FROM \'"
        + list.get(0) + "\'";
    for (int i = 1; i < list.size(); i++) {
      sqlUnion += "UNION ALL" +
          "SELECT fid, count FROM \'" + list.get(i) + "\'";
    }
    String sqlSufix = ") GROUP BY fid ";
    // TODO: safe check
    String sqlCountFilter =
        (countFilter == null || countFilter.length() == 0) ?
            "" :
            "HAVING SUM(count) " + countFilter;
    String sqlFinal = sqlPrefix + sqlUnion + sqlSufix + sqlCountFilter;
    SqlRowSet sqlRowSet = jdbcTemplate.queryForRowSet(sqlFinal);
    while(sqlRowSet.next()) {
      ret.put(sqlRowSet.getLong("fid"), sqlRowSet.getInt("count"));
    }
    return ret;
  }*/

  public Map<Long, Integer> getHotFiles(List<AccessCountTable> tables,
                                        int topNum) throws SQLException {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    Iterator<AccessCountTable> tableIterator = tables.iterator();
    StringBuilder unioned = new StringBuilder();
    while (tableIterator.hasNext()) {
      AccessCountTable table = tableIterator.next();
      if (tableIterator.hasNext()) {
        unioned
            .append("SELECT * FROM " + table.getTableName() + " UNION ALL ");
      } else {
        unioned.append("SELECT * FROM " + table.getTableName());
      }
    }
    String statement =
        String.format(
            "SELECT %s, SUM(%s) as %s FROM (%s) tmp GROUP BY %s ORDER BY %s DESC LIMIT %s",
            AccessCountDao.FILE_FIELD,
            AccessCountDao.ACCESSCOUNT_FIELD,
            AccessCountDao.ACCESSCOUNT_FIELD,
            unioned,
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


  public void createProportionView(AccessCountTable dest, AccessCountTable source)
      throws SQLException {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);;
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

  class AccessCountRowMapper implements RowMapper<AccessCountTable> {

    @Override
    public AccessCountTable mapRow(ResultSet resultSet, int i) throws SQLException {
      AccessCountTable accessCountTable = new AccessCountTable(
          resultSet.getLong("start_time"),
          resultSet.getLong("end_time") );
      return accessCountTable;
    }
  }
}
