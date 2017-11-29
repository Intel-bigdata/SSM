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
import org.smartdata.model.FileDiff;
import org.smartdata.model.FileDiffState;
import org.smartdata.model.FileDiffType;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;

import javax.sql.DataSource;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FileDiffDao {
  private static final String TABLE_NAME = "file_diff";
  private DataSource dataSource;

  public void setDataSource(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public FileDiffDao(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public List<FileDiff> getAll() {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.query("SELECT * FROM " + TABLE_NAME, new FileDiffRowMapper());
  }

  public List<FileDiff> getPendingDiff() {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.query(
        "SELECT * FROM " + TABLE_NAME + " WHERE state = 0", new FileDiffRowMapper());
  }

  public List<FileDiff> getByState(FileDiffState fileDiffState) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate
        .query("SELECT * FROM " + TABLE_NAME + " WHERE state = ?",
            new Object[]{fileDiffState.getValue()}, new FileDiffRowMapper());
  }

  public List<FileDiff> getByState(String prefix, FileDiffState fileDiffState) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate
        .query(
            "SELECT * FROM " + TABLE_NAME + " WHERE src LIKE ? and state = ?",
            new Object[]{prefix + "%", fileDiffState.getValue()},
            new FileDiffRowMapper());
  }

  public List<FileDiff> getPendingDiff(long rid) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.query("SELECT * FROM " + TABLE_NAME + " WHERE did = ? and state = 0",
        new Object[]{rid},
        new FileDiffRowMapper());
  }

  public List<FileDiff> getPendingDiff(String prefix) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.query("SELECT * FROM " + TABLE_NAME + " WHERE src LIKE ? and state = 0",
        new FileDiffRowMapper(), prefix + "%");
  }

  public List<FileDiff> getByIds(List<Long> dids) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.query("SELECT * FROM " + TABLE_NAME + " WHERE did IN (?)",
        new Object[]{StringUtils.join(dids, ",")},
        new FileDiffRowMapper());
  }

  public List<FileDiff> getByFileName(String fileName) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.query("SELECT * FROM " + TABLE_NAME + " WHERE src = ?",
        new Object[]{fileName}, new FileDiffRowMapper());
  }

  public List<String> getSyncPath(int size) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    if (size != 0) {
      jdbcTemplate.setMaxRows(size);
    }
    String sql = "SELECT DISTINCT src FROM " + TABLE_NAME + " WHERE state = ?";
    return jdbcTemplate
        .queryForList(sql, String.class, FileDiffState.RUNNING.getValue());
  }


  public FileDiff getById(long did) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.queryForObject("SELECT * FROM " + TABLE_NAME + " WHERE did = ?",
        new Object[]{did}, new FileDiffRowMapper());
  }

  public void delete(long did) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    final String sql = "DELETE FROM " + TABLE_NAME + " WHERE did = ?";
    jdbcTemplate.update(sql, did);
  }

  public long insert(FileDiff fileDiff) {
    SimpleJdbcInsert simpleJdbcInsert = new SimpleJdbcInsert(dataSource);
    simpleJdbcInsert.setTableName(TABLE_NAME);
    simpleJdbcInsert.usingGeneratedKeyColumns("did");
    // return did
    long did = simpleJdbcInsert.executeAndReturnKey(toMap(fileDiff)).longValue();
    fileDiff.setDiffId(did);
    return did;
  }

  public void insert(FileDiff[] fileDiffs) {
    SimpleJdbcInsert simpleJdbcInsert = new SimpleJdbcInsert(dataSource);
    simpleJdbcInsert.setTableName(TABLE_NAME);
    Map[] maps = new Map[fileDiffs.length];
    for (int i = 0; i < fileDiffs.length; i++) {
      maps[i] = toMap(fileDiffs[i]);
    }
    simpleJdbcInsert.executeBatch(maps);
  }

  public Long[] insert(List<FileDiff> fileDiffs) {
    List<Long> dids = new ArrayList<>();
    for (FileDiff fileDiff : fileDiffs) {
      dids.add(insert(fileDiff));
    }
    return dids.toArray(new Long[dids.size()]);
  }

  public int[] batchUpdate(
      final List<Long> dids, final List<FileDiffState> states, final List<String> parameters) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);

    final String sql = "UPDATE " + TABLE_NAME + " SET state = ?, "
        + "parameters = ? WHERE did = ?";
    return jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
      @Override
      public void setValues(PreparedStatement ps, int i) throws SQLException {
        ps.setShort(1, (short) states.get(i).getValue());
        ps.setString(2, parameters.get(i));
        ps.setLong(3, dids.get(i));
      }

      @Override
      public int getBatchSize() {
        return dids.size();
      }
    });
  }

  public int update(long did, FileDiffState state) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String sql = "UPDATE " + TABLE_NAME + " SET state = ? WHERE did = ?";
    return jdbcTemplate.update(sql, state.getValue(), did);
  }

  public int update(long did, String src) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String sql = "UPDATE " + TABLE_NAME + " SET src = ? WHERE did = ?";
    return jdbcTemplate.update(sql, src, did);
  }

  public int update(long did, FileDiffState state,
       String parameters) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String sql = "UPDATE " + TABLE_NAME + " SET state = ?, "
        + "parameters = ? WHERE did = ?";
    return jdbcTemplate.update(sql, state.getValue(), parameters, did);
  }

  public int update(final FileDiff fileDiff) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String sql = "UPDATE " + TABLE_NAME + " SET "
        + "rid = ?, "
        + "diff_type = ?, "
        + "src = ?, "
        + "parameters = ?, "
        + "state = ?, "
        + "create_time = ? "
        + "WHERE did = ?";
    return jdbcTemplate.update(sql, fileDiff.getRuleId(),
        fileDiff.getDiffType().getValue(), fileDiff.getSrc(),
        fileDiff.getParametersJsonString(), fileDiff.getState().getValue(),
        fileDiff.getCreateTime(), fileDiff.getDiffId());
  }


  public void deleteAll() {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    final String sql = "DELETE FROM " + TABLE_NAME;
    jdbcTemplate.execute(sql);
  }

  private Map<String, Object> toMap(FileDiff fileDiff) {
    // System.out.println(fileDiff.getDiffType());
    Map<String, Object> parameters = new HashMap<>();
    parameters.put("did", fileDiff.getDiffId());
    parameters.put("rid", fileDiff.getRuleId());
    parameters.put("diff_type", fileDiff.getDiffType().getValue());
    parameters.put("src", fileDiff.getSrc());
    parameters.put("parameters", fileDiff.getParametersJsonString());
    parameters.put("state", fileDiff.getState().getValue());
    parameters.put("create_time", fileDiff.getCreateTime());
    return parameters;
  }

  class FileDiffRowMapper implements RowMapper<FileDiff> {
    @Override
    public FileDiff mapRow(ResultSet resultSet, int i) throws SQLException {
      FileDiff fileDiff = new FileDiff();
      fileDiff.setDiffId(resultSet.getLong("did"));
      fileDiff.setRuleId(resultSet.getLong("rid"));
      fileDiff.setDiffType(FileDiffType.fromValue((int) resultSet.getByte("diff_type")));
      fileDiff.setSrc(resultSet.getString("src"));
      fileDiff.setParametersFromJsonString(resultSet.getString("parameters"));
      fileDiff.setState(FileDiffState.fromValue((int) resultSet.getByte("state")));
      fileDiff.setCreateTime(resultSet.getLong("create_time"));
      return fileDiff;
    }
  }
}
