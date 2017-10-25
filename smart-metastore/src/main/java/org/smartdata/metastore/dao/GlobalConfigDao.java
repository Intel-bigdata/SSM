/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.smartdata.metastore.dao;

import org.apache.commons.lang.StringUtils;
import org.smartdata.model.GlobalConfig;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;

import javax.sql.DataSource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GlobalConfigDao {
  private DataSource dataSource;

  public void setDataSource(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public GlobalConfigDao(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public List<GlobalConfig> getAll() {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.query("SELECT * FROM global_config", new GlobalConfigRowMapper());
  }

  public List<GlobalConfig> getByIds(List<Long> cid) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.query(
        "SELECT * FROM global_config WHERE cid IN (?)",
        new Object[] {StringUtils.join(cid, ",")},
        new GlobalConfigRowMapper());
  }

  public GlobalConfig getById(long cid) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.queryForObject(
        "SELECT * FROM global_config WHERE cid = ?",
        new Object[] {cid},
        new GlobalConfigRowMapper());
  }

  public GlobalConfig getByPropertyName(String propertyName) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.queryForObject(
        "SELECT * FROM global_config WHERE property_name = ?",
        new Object[] {propertyName},
        new GlobalConfigRowMapper());
  }

  public void deleteByName(String propertyName) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    final String sql = "DELETE FROM global_config WHERE property_name = ?";
    jdbcTemplate.update(sql, propertyName);
  }

  public void delete(long cid) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    final String sql = "DELETE FROM global_config WHERE cid = ?";
    jdbcTemplate.update(sql, cid);
  }

  public long insert(GlobalConfig globalConfig) {
    SimpleJdbcInsert simpleJdbcInsert = new SimpleJdbcInsert(dataSource);
    simpleJdbcInsert.setTableName("global_config");
    simpleJdbcInsert.usingGeneratedKeyColumns("cid");
    long cid = simpleJdbcInsert.executeAndReturnKey(toMaps(globalConfig)).longValue();
    globalConfig.setCid(cid);
    return cid;
  }

  // TODO slove the increment of key
  public void insert(GlobalConfig[] globalConfigs) {
    SimpleJdbcInsert simpleJdbcInsert = new SimpleJdbcInsert(dataSource);
    simpleJdbcInsert.setTableName("global_config");
    simpleJdbcInsert.usingGeneratedKeyColumns("cid");
    Map<String, Object>[] maps = new Map[globalConfigs.length];
    for (int i = 0; i < globalConfigs.length; i++) {
      maps[i] = toMaps(globalConfigs[i]);
    }

    int[] cids = simpleJdbcInsert.executeBatch(maps);
    for (int i = 0; i < globalConfigs.length; i++) {
      globalConfigs[i].setCid(cids[i]);
    }
  }

  public long getCountByName(String name) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.queryForObject(
        "SELECT COUNT(*) FROM global_config WHERE property_name = ?", Long.class, name);
  }

  public int update(String propertyName, String propertyValue) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String sql = "UPDATE global_config SET property_value = ? WHERE property_name = ?";
    return jdbcTemplate.update(sql, propertyValue, propertyName);
  }

  private Map<String, Object> toMaps(GlobalConfig globalConfig) {
    Map<String, Object> parameters = new HashMap<>();
    parameters.put("cid", globalConfig.getCid());
    parameters.put("property_name", globalConfig.getPropertyName());
    parameters.put("property_value", globalConfig.getPropertyValue().toString());
    return parameters;
  }

  class GlobalConfigRowMapper implements RowMapper<GlobalConfig> {

    @Override
    public GlobalConfig mapRow(ResultSet resultSet, int i) throws SQLException {
      GlobalConfig globalConfig = new GlobalConfig();
      globalConfig.setCid(resultSet.getLong("cid"));
      globalConfig.setPropertyName(resultSet.getString("property_name"));
      globalConfig.setPropertyValue(resultSet.getString("property_value"));
      return globalConfig;
    }
  }
}
