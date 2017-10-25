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
import org.smartdata.model.ClusterInfo;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;

import javax.sql.DataSource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClusterInfoDao {
  private static final String TABLE_NAME = "cluster_info";
  private DataSource dataSource;

  public void setDataSource(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public ClusterInfoDao(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public List<ClusterInfo> getAll() {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.query("SELECT * FROM " + TABLE_NAME, new ClusterinfoRowMapper());
  }

  public ClusterInfo getById(long cid) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.query("SELECT * FROM " + TABLE_NAME + " WHERE cid = ?",
        new Object[]{cid},
        new ClusterinfoRowMapper()).get(0);
  }

  public List<ClusterInfo> getByIds(List<Long> cids) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.query("SELECT * FROM " + TABLE_NAME + " WHERE cid IN (?)",
        new Object[]{StringUtils.join(cids, ",")},
        new ClusterinfoRowMapper());
  }

  public void delete(long cid) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    final String sql = "DELETE FROM " + TABLE_NAME + " WHERE cid = ?";
    jdbcTemplate.update(sql, cid);
  }

  public long insert(ClusterInfo clusterInfo) {
    SimpleJdbcInsert simpleJdbcInsert = new SimpleJdbcInsert(dataSource);
    simpleJdbcInsert.setTableName(TABLE_NAME);
    simpleJdbcInsert.usingGeneratedKeyColumns("cid");
    long cid = simpleJdbcInsert.executeAndReturnKey(toMap(clusterInfo)).longValue();
    clusterInfo.setCid(cid);
    return cid;
  }

  public void insert(ClusterInfo[] clusterInfos) {
    SimpleJdbcInsert simpleJdbcInsert = new SimpleJdbcInsert(dataSource);
    simpleJdbcInsert.setTableName("cluster_info");
    simpleJdbcInsert.usingGeneratedKeyColumns("cid");
    Map<String, Object>[] maps = new Map[clusterInfos.length];
    for (int i = 0; i < clusterInfos.length; i++) {
      maps[i] = toMap(clusterInfos[i]);
    }

    int[] cids = simpleJdbcInsert.executeBatch(maps);
    for (int i = 0; i < clusterInfos.length; i++) {
      clusterInfos[i].setCid(cids[i]);
    }
  }

  public int updateState(long cid, String state) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String sql = "UPDATE " + TABLE_NAME + " SET state = ? WHERE cid = ?";
    return jdbcTemplate.update(sql, state, cid);
  }

  public int updateType(long cid, String type) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String sql = "UPDATE " + TABLE_NAME + " SET type = ? WHERE cid = ?";
    return jdbcTemplate.update(sql, type, cid);
  }

  public void deleteAll() {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    final String sql = "DELETE FROM " + TABLE_NAME;
    jdbcTemplate.execute(sql);
  }

  public int getCountByName(String name) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    final String sql = "SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE name = ?";
    return jdbcTemplate.queryForObject(sql, Integer.class, name);
  }

  private Map<String, Object> toMap(ClusterInfo clusterInfo) {
    Map<String, Object> parameters = new HashMap<>();
    parameters.put("cid", clusterInfo.getCid());
    parameters.put("name", clusterInfo.getName());
    parameters.put("url", clusterInfo.getUrl());
    parameters.put("conf_path", clusterInfo.getConfPath());
    parameters.put("state", clusterInfo.getState());
    parameters.put("type", clusterInfo.getType());

    return parameters;
  }

  class ClusterinfoRowMapper implements RowMapper<ClusterInfo> {

    @Override
    public ClusterInfo mapRow(ResultSet resultSet, int i) throws SQLException {
      ClusterInfo clusterInfo = new ClusterInfo();
      clusterInfo.setCid(resultSet.getLong("cid"));
      clusterInfo.setName(resultSet.getString("name"));
      clusterInfo.setUrl(resultSet.getString("url"));
      clusterInfo.setConfPath(resultSet.getString("conf_path"));
      clusterInfo.setState(resultSet.getString("state"));
      clusterInfo.setType(resultSet.getString("type"));

      return clusterInfo;
    }
  }
}
