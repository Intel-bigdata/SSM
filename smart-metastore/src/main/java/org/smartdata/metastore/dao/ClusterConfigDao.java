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
import org.smartdata.model.ClusterConfig;
import org.smartdata.model.CmdletInfo;
import org.smartdata.model.FileDiff;
import org.smartdata.model.FileDiffType;
import org.smartdata.model.FileInfo;
import org.smartdata.metastore.utils.MetaStoreUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;
import org.sqlite.JDBC;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClusterConfigDao {
  private DataSource dataSource;

  public void setDataSource(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public ClusterConfigDao(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public List<ClusterConfig> getAll() {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.query("select * from cluster_config", new ClusterConfigRowMapper());
  }

  public List<ClusterConfig> getByIds(List<Long> cids) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.query("select * from cluster_config WHERE cid IN (?)",
        new Object[]{StringUtils.join(cids, ",")},
        new ClusterConfigRowMapper());
  }

  public ClusterConfig getById(long cid) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.queryForObject("select * from cluster_config WHERE cid = ?",
        new Object[]{cid}, new ClusterConfigRowMapper());
  }

  public long getCountByName(String name) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.queryForObject("select COUNT(*) FROM cluster_config WHERE node_name = ?",Long.class,name);
  }

  public ClusterConfig getByName(String name) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.queryForObject("select * from cluster_config WHERE node_name = ?",
        new Object[]{name}, new ClusterConfigRowMapper());
  }

  public void delete(long cid) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    final String sql = "delete from cluster_config where cid = ?";
    jdbcTemplate.update(sql, cid);
  }

  public long insert(ClusterConfig clusterConfig) {
    SimpleJdbcInsert simpleJdbcInsert = new SimpleJdbcInsert(dataSource);
    simpleJdbcInsert.setTableName("cluster_config");
    simpleJdbcInsert.usingGeneratedKeyColumns("cid");
    long cid = simpleJdbcInsert.executeAndReturnKey(toMap(clusterConfig)).longValue();
    clusterConfig.setCid(cid);
    return cid;
  }

  // TODO slove the increment of key
  public void insert(ClusterConfig[] clusterConfigs) {
    SimpleJdbcInsert simpleJdbcInsert = new SimpleJdbcInsert(dataSource);
    simpleJdbcInsert.setTableName("cluster_config");
    simpleJdbcInsert.usingGeneratedKeyColumns("cid");
    Map<String, Object>[] maps = new Map[clusterConfigs.length];
    for (int i = 0; i < clusterConfigs.length; i++) {
      maps[i] = toMap(clusterConfigs[i]);
    }
    int[] cids = simpleJdbcInsert.executeBatch(maps);

    for (int i = 0; i < clusterConfigs.length; i++) {
      clusterConfigs[i].setCid(cids[i]);
    }
  }

  public int updateById(int cid, String config_path){
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    final String sql = "update cluster_config set config_path = ? WHERE cid = ?";
    return jdbcTemplate.update(sql,config_path,cid);
  }


  public int updateByNodeName(String node_name, String config_path){
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    final String sql = "update cluster_config set config_path = ? WHERE node_name = ?";
    return jdbcTemplate.update(sql,config_path,node_name);
  }


  private Map<String, Object> toMap(ClusterConfig clusterConfig) {
    Map<String, Object> parameters = new HashMap<>();
    parameters.put("cid", clusterConfig.getCid());
    parameters.put("config_path", clusterConfig.getConfigPath());
    parameters.put("node_name", clusterConfig.getNodeName());
    return parameters;
  }

  class ClusterConfigRowMapper implements RowMapper<ClusterConfig> {

    @Override
    public ClusterConfig mapRow(ResultSet resultSet, int i) throws SQLException {
      ClusterConfig clusterConfig = new ClusterConfig();
      clusterConfig.setCid(resultSet.getLong("cid"));
      clusterConfig.setConfig_path(resultSet.getString("config_path"));
      clusterConfig.setNodeName(resultSet.getString("node_name"));

      return clusterConfig;
    }
  }

}
