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
import org.smartdata.model.BackUpInfo;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;

import javax.sql.DataSource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BackUpInfoDao {
  private DataSource dataSource;

  public void setDataSource(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public BackUpInfoDao(DataSource dataSource){
    this.dataSource = dataSource;
  }

  public List<BackUpInfo> getAll() {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.query("SELECT * FROM backup_file", new BackUpInfoRowMapper());
  }

  public List<BackUpInfo> getByIds(List<Long> rids) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.query("SELECT * FROM backup_file WHERE rid IN (?)",
        new Object[]{StringUtils.join(rids, ",")},
        new BackUpInfoRowMapper());
  }

  public int getCountByRid(int rid){
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.queryForObject(
        "SELECT COUNT(*) FROM backup_file WHERE rid = ?", new Object[rid], Integer.class);
  }

  public BackUpInfo getByRid(long rid) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.queryForObject("SELECT * FROM backup_file WHERE rid = ?",
        new Object[]{rid}, new BackUpInfoRowMapper());
  }

  public List<BackUpInfo> getBySrc(String src) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.query(
        "SELECT * FROM backup_file WHERE src = ?", new Object[] {src}, new BackUpInfoRowMapper());
  }

  public List<BackUpInfo> getByDest(String dest) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.query(
        "SELECT * FROM backup_file WHERE dest = ?", new Object[] {dest}, new BackUpInfoRowMapper());
  }


  public void delete(long rid) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    final String sql = "DELETE FROM backup_file WHERE rid = ?";
    jdbcTemplate.update(sql, rid);
  }

  public void insert(BackUpInfo backUpInfo) {
    SimpleJdbcInsert simpleJdbcInsert = new SimpleJdbcInsert(dataSource);
    simpleJdbcInsert.setTableName("backup_file");
    simpleJdbcInsert.execute(toMap(backUpInfo));
  }

  public void insert(BackUpInfo[] backUpInfos) {
    SimpleJdbcInsert simpleJdbcInsert = new SimpleJdbcInsert(dataSource);
    simpleJdbcInsert.setTableName("backup_file");
    Map<String, Object>[] maps = new Map[backUpInfos.length];
    for (int i = 0; i < backUpInfos.length; i++) {
      maps[i] = toMap(backUpInfos[i]);
    }
    simpleJdbcInsert.executeBatch(maps);
  }

  public int update(long rid, long period) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String sql = "UPDATE backup_file SET period = ? WHERE rid = ?";
    return jdbcTemplate.update(sql, period, rid);
  }

  public void deleteAll(){
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    final String sql = "DELETE FROM backup_file";
    jdbcTemplate.execute(sql);
  }

  private Map<String, Object> toMap(BackUpInfo backUpInfo) {
    Map<String, Object> parameters = new HashMap<>();
    parameters.put("rid", backUpInfo.getRid());
    parameters.put("src", backUpInfo.getSrc());
    parameters.put("dest", backUpInfo.getDest());
    parameters.put("period", backUpInfo.getPeriod());
    return parameters;
  }

  class BackUpInfoRowMapper implements RowMapper<BackUpInfo> {

    @Override
    public BackUpInfo mapRow(ResultSet resultSet, int i) throws SQLException {
      BackUpInfo backUpInfo = new BackUpInfo();
      backUpInfo.setRid(resultSet.getLong("rid"));
      backUpInfo.setSrc(resultSet.getString("src"));
      backUpInfo.setDest(resultSet.getString("dest"));
      backUpInfo.setPeriod(resultSet.getLong("period"));

      return backUpInfo;
    }
  }
}
