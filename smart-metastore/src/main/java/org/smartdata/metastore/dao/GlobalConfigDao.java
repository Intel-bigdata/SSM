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
import org.smartdata.model.CmdletInfo;
import org.smartdata.model.FileDiff;
import org.smartdata.model.FileDiffType;
import org.smartdata.model.FileInfo;
import org.smartdata.metastore.utils.MetaStoreUtils;
import org.smartdata.model.GlobalConfig;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
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
    return jdbcTemplate.query("select * from global_config", new GlobalConfigRowMapper());
  }

  public List<GlobalConfig> getByIds(List<Long> cid) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.query("select * from global_config WHERE cid IN (?)",
        new Object[]{StringUtils.join(cid, ",")},
        new GlobalConfigRowMapper());
  }

  public GlobalConfig getById(long cid) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.queryForObject("select * from global_config WHERE cid = ?",
        new Object[]{cid}, new GlobalConfigRowMapper());
  }

  public GlobalConfig getByPropertyName(String property_name) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.queryForObject("select * from global_config WHERE property_name = ?",
        new Object[]{property_name}, new GlobalConfigRowMapper());
  }

  public void deleteByName(String property_name) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    final String sql = "delete from global_config where property_name = ?";
    jdbcTemplate.update(sql, property_name);
  }

  public void delete(long cid) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    final String sql = "delete from global_config where cid = ?";
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
    return jdbcTemplate.queryForObject("select COUNT(*) FROM global_config WHERE property_name = ?",Long.class,name);
  }

  public int update(String property_name, String property_value) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String sql = "update global_config set property_value = ? WHERE property_name = ?";
    return jdbcTemplate.update(sql, property_value, property_name);
  }


  private Map<String, Object> toMaps(GlobalConfig globalConfig) {
    Map<String, Object> parameters = new HashMap<>();
    parameters.put("cid", globalConfig.getCid());
    parameters.put("property_name", globalConfig.getProperty_name());
    parameters.put("property_value", globalConfig.getProperty_value().toString());
    return parameters;
  }

  class GlobalConfigRowMapper implements RowMapper<GlobalConfig> {

    @Override
    public GlobalConfig mapRow(ResultSet resultSet, int i) throws SQLException {
      GlobalConfig globalConfig = new GlobalConfig();
      globalConfig.setCid(resultSet.getLong("cid"));
      globalConfig.setProperty_name(resultSet.getString("property_name"));
      globalConfig.setProperty_value(resultSet.getString("property_value"));
      return globalConfig;
    }
  }
}
