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

public class FileDiffDao {
  private DataSource dataSource;

  public void setDataSource(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public FileDiffDao(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public List<FileDiff> getALL() {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.query("select * from file_diff", new FileDiffRowMapper());
  }

  public List<FileDiff> getByIds(List<Long> dids) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.query("select * from file_diff WHERE did IN (?)",
        new Object[]{StringUtils.join(dids, ",")},
        new FileDiffRowMapper());
  }

  public FileDiff getById(long did) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.queryForObject("select * from file_diff where did = ?",
        new Object[]{did}, new FileDiffRowMapper());
  }

  public void delete(long did) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    final String sql = "delete from file_diff where did = ?";
    jdbcTemplate.update(sql, did);
  }

  public long insert(FileDiff fileDiff) {
    SimpleJdbcInsert simpleJdbcInsert = new SimpleJdbcInsert(dataSource);
    simpleJdbcInsert.setTableName("file_diff");
    simpleJdbcInsert.usingGeneratedKeyColumns("did");
    // return did
    return simpleJdbcInsert.executeAndReturnKey(toMap(fileDiff)).longValue();
  }

  public void insert(FileDiff[] fileDiffs) {
    SimpleJdbcInsert simpleJdbcInsert = new SimpleJdbcInsert(dataSource);
    simpleJdbcInsert.setTableName("file_diff");
    Map<String, Object>[] maps = new Map[fileDiffs.length];
    for (int i = 0; i < fileDiffs.length; i++) {
      maps[i] = toMap(fileDiffs[i]);
    }
    simpleJdbcInsert.executeBatch(maps);
  }

  public int update(long did, boolean newApplied) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String sql = "update file_diff set applied = ? WHERE did = ?";
    return jdbcTemplate.update(sql, newApplied, did);
  }


  public void deleteAll() {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    final String sql = "DELETE from file_diff";
    jdbcTemplate.execute(sql);
  }

  private Map<String, Object> toMap(FileDiff fileDiff) {
    // System.out.println(fileDiff.getDiffType());
    Map<String, Object> parameters = new HashMap<>();
    parameters.put("did", fileDiff.getDiffId());
    parameters.put("diff_type", fileDiff.getDiffType().getValue());
    parameters.put("parameters", fileDiff.getParameters());
    parameters.put("applied", fileDiff.isApplied());
    parameters.put("create_time", fileDiff.getCreate_time());
    return parameters;
  }

  class FileDiffRowMapper implements RowMapper<FileDiff> {
    @Override
    public FileDiff mapRow(ResultSet resultSet, int i) throws SQLException {
      FileDiff fileDiff = new FileDiff();
      fileDiff.setDiffId(resultSet.getLong("did"));
      fileDiff.setDiffType(FileDiffType.fromValue((int) resultSet.getByte("diff_type")));
      fileDiff.setApplied(resultSet.getBoolean("applied"));
      fileDiff.setCreate_time(resultSet.getLong("create_time"));
      fileDiff.setParameters(resultSet.getString("parameters"));

      return fileDiff;
    }
  }
}
