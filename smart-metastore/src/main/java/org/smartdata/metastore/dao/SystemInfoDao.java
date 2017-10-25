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

import org.apache.commons.lang.StringUtils;
import org.smartdata.model.SystemInfo;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;

import javax.sql.DataSource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SystemInfoDao {
  private static final String TABLE_NAME = "sys_info";
  private DataSource dataSource;

  public void setDataSource(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public SystemInfoDao(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public List<SystemInfo> getAll() {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.query("SELECT * FROM " + TABLE_NAME, new SystemInfoRowMapper());
  }

  public boolean containsProperty(String property) {
    return !list(property).isEmpty();
  }

  private List<SystemInfo> list(String property) {
    return new JdbcTemplate(dataSource)
        .query(
            "SELECT * FROM " + TABLE_NAME + " WHERE property = ?",
            new Object[] {property},
            new SystemInfoRowMapper());
  }

  public SystemInfo getByProperty(String property) {
    List<SystemInfo> infos = list(property);
    return infos.isEmpty() ? null : infos.get(0);
  }

  public List<SystemInfo> getByProperties(List<String> properties) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.query("SELECT * FROM " + TABLE_NAME + " WHERE property IN (?)",
        new Object[]{StringUtils.join(properties, ",")},
        new SystemInfoRowMapper());
  }

  public void delete(String property) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    final String sql = "DELETE FROM " + TABLE_NAME + " WHERE property = ?";
    jdbcTemplate.update(sql, property);
  }

  public void insert(SystemInfo systemInfo) {
    SimpleJdbcInsert simpleJdbcInsert = new SimpleJdbcInsert(dataSource);
    simpleJdbcInsert.setTableName(TABLE_NAME);
    simpleJdbcInsert.execute(toMap(systemInfo));
  }

  public void insert(SystemInfo[] systemInfos) {
    SimpleJdbcInsert simpleJdbcInsert = new SimpleJdbcInsert(dataSource);
    simpleJdbcInsert.setTableName(TABLE_NAME);

    Map<String, Object>[] maps = new Map[systemInfos.length];
    for (int i = 0; i < systemInfos.length; i++){
      maps[i] = toMap(systemInfos[i]);
    }

    simpleJdbcInsert.executeBatch(maps);
  }

  public int update(SystemInfo systemInfo) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String sql = "UPDATE " + TABLE_NAME + " SET value = ? WHERE property = ?";
    return jdbcTemplate.update(sql, systemInfo.getValue(), systemInfo.getProperty());
  }

  public void deleteAll() {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    final String sql = "DELETE FROM " + TABLE_NAME;
    jdbcTemplate.execute(sql);
  }

  private Map<String, Object> toMap(SystemInfo systemInfo) {
    Map<String, Object> parameters = new HashMap<>();
    parameters.put("property", systemInfo.getProperty());
    parameters.put("value", systemInfo.getValue());
    return parameters;
  }

  class SystemInfoRowMapper implements RowMapper<SystemInfo> {

    @Override
    public SystemInfo mapRow(ResultSet resultSet, int i) throws SQLException {
      return new SystemInfo(resultSet.getString("property"), resultSet.getString("value"));
    }
  }
}
