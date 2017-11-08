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

import org.apache.commons.lang.StringEscapeUtils;
import org.smartdata.model.ActionInfo;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;

import javax.sql.DataSource;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ActionDao {

  private static final String TABLE_NAME = "action";
  private DataSource dataSource;

  public void setDataSource(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public ActionDao(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public List<ActionInfo> getAll() {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.query("SELECT * FROM " + TABLE_NAME,
        new ActionRowMapper());
  }

  public Long getCountOfAction() {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String sql = "SELECT COUNT(*) FROM " + TABLE_NAME;
    return jdbcTemplate.queryForObject(sql, Long.class);
  }

  public ActionInfo getById(long aid) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.queryForObject(
        "SELECT * FROM " + TABLE_NAME + " WHERE aid = ?",
        new Object[] {aid},
        new ActionRowMapper());
  }

  public List<ActionInfo> getByIds(List<Long> aids) {
    NamedParameterJdbcTemplate namedParameterJdbcTemplate =
        new NamedParameterJdbcTemplate(dataSource);
    MapSqlParameterSource parameterSource = new MapSqlParameterSource();
    parameterSource.addValue("aids", aids);
    return namedParameterJdbcTemplate.query(
        "SELECT * FROM " + TABLE_NAME + " WHERE aid IN (:aids)",
        parameterSource,
        new ActionRowMapper());
  }

  public List<ActionInfo> getByCid(long cid) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.query(
        "SELECT * FROM " + TABLE_NAME + " WHERE cid = ?",
        new Object[] {cid},
        new ActionRowMapper());
  }

  public List<ActionInfo> getByCondition(String aidCondition,
      String cidCondition) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String sqlPrefix = "SELECT * FROM " + TABLE_NAME + " WHERE ";
    String sqlAid = (aidCondition == null) ? "" : "AND aid " + aidCondition;
    String sqlCid = (cidCondition == null) ? "" : "AND cid " + cidCondition;
    String sqlFinal = "";
    if (aidCondition != null || cidCondition != null) {
      sqlFinal = sqlPrefix + sqlAid + sqlCid;
      sqlFinal = sqlFinal.replaceFirst("AND ", "");
    } else {
      sqlFinal = sqlPrefix.replaceFirst("WHERE ", "");
    }
    return jdbcTemplate.query(sqlFinal, new ActionRowMapper());
  }

  public List<ActionInfo> getLatestActions(int size) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    if (size != 0) {
      jdbcTemplate.setMaxRows(size);
    }
    String sql = "SELECT * FROM " + TABLE_NAME + " ORDER BY aid DESC";
    return jdbcTemplate.query(sql, new ActionRowMapper());
  }

  public List<ActionInfo> getLatestActions(String actionName, int size) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    if (size != 0) {
      jdbcTemplate.setMaxRows(size);
    }
    String sql = "SELECT * FROM " + TABLE_NAME + " WHERE action_name = ? ORDER BY aid DESC";
    return jdbcTemplate.query(sql, new ActionRowMapper(), actionName);
  }

  public List<ActionInfo> getLatestActions(String actionName, int size,
      boolean successful, boolean finished) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    if (size != 0) {
      jdbcTemplate.setMaxRows(size);
    }
    String sql =
        "SELECT * FROM "
            + TABLE_NAME
            + " WHERE action_name = ? AND successful = ? AND finished = ? ORDER BY aid DESC";
    return jdbcTemplate.query(sql, new ActionRowMapper(), actionName, successful, finished);
  }

  public List<ActionInfo> getLatestActions(String actionName, boolean successful,
      int size) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    if (size != 0) {
      jdbcTemplate.setMaxRows(size);
    }
    String sql =
        "SELECT * FROM "
            + TABLE_NAME
            + " WHERE action_name = ? AND successful = ? ORDER BY aid DESC";
    return jdbcTemplate.query(sql, new ActionRowMapper(), actionName, successful);
  }

  public List<ActionInfo> getAPageOfAction(long start, long offset, List<String> orderBy,
      List<Boolean> isDesc) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    boolean ifHasAid = false;
    String sql = "SELECT * FROM " + TABLE_NAME + " ORDER BY ";

    for (int i = 0; i < orderBy.size(); i++) {
      if (orderBy.get(i).equals("aid")) {
        ifHasAid = true;
      }
      sql = sql + orderBy.get(i);
      if (isDesc.size() > i) {
        if (isDesc.get(i)) {
          sql = sql + " desc ";
        }
        sql = sql + ",";
      }
    }

    if (!ifHasAid) {
      sql = sql + "aid,";
    }

    //delete the last char
    sql = sql.substring(0, sql.length() - 1);
    //add limit
    sql = sql + " LIMIT " + start + "," + offset + ";";
    return jdbcTemplate.query(sql, new ActionRowMapper());
  }

  public List<ActionInfo> getAPageOfAction(long start, long offset) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String sql = "SELECT * FROM " + TABLE_NAME + " LIMIT " + start + "," + offset + ";";
    return jdbcTemplate.query(sql, new ActionRowMapper());
  }

  public List<ActionInfo> getLatestActions(String actionType, int size,
      boolean finished) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    if (size != 0) {
      jdbcTemplate.setMaxRows(size);
    }
    String sql =
        "SELECT * FROM " + TABLE_NAME + " WHERE action_name = ? AND finished = ? ORDER BY aid DESC";
    return jdbcTemplate.query(sql, new ActionRowMapper(), actionType, finished);
  }

  public void delete(long aid) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    final String sql = "DELETE FROM " + TABLE_NAME + " WHERE aid = ?";
    jdbcTemplate.update(sql, aid);
  }

  public void deleteCmdletActions(long cid) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    final String sql = "DELETE FROM " + TABLE_NAME + " WHERE cid = ?";
    jdbcTemplate.update(sql, cid);
  }

  public void deleteAll() {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    final String sql = "DELETE FROM " + TABLE_NAME;
    jdbcTemplate.execute(sql);
  }

  public void insert(ActionInfo actionInfo) {
    SimpleJdbcInsert simpleJdbcInsert = new SimpleJdbcInsert(dataSource);
    simpleJdbcInsert.setTableName(TABLE_NAME);
    simpleJdbcInsert.execute(toMap(actionInfo));
  }

  public void insert(ActionInfo[] actionInfos) {
    SimpleJdbcInsert simpleJdbcInsert = new SimpleJdbcInsert(dataSource);
    simpleJdbcInsert.setTableName(TABLE_NAME);
    Map<String, Object>[] maps = new Map[actionInfos.length];
    for (int i = 0; i < actionInfos.length; i++) {
      maps[i] = toMap(actionInfos[i]);
    }
    simpleJdbcInsert.executeBatch(maps);
  }

  public int update(final ActionInfo actionInfo) {
    return update(new ActionInfo[]{actionInfo})[0];
  }

  public int[] update(final ActionInfo[] actionInfos) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String sql =
        "UPDATE "
            + TABLE_NAME
            + " SET "
            + "result = ?, "
            + "log = ?, "
            + "successful = ?, "
            + "create_time = ?, "
            + "finished = ?, "
            + "finish_time = ?, "
            + "progress = ? "
            + "WHERE aid = ?";
    return jdbcTemplate.batchUpdate(sql,
        new BatchPreparedStatementSetter() {
          public void setValues(PreparedStatement ps,
              int i) throws SQLException {
            ps.setString(1, actionInfos[i].getResult());
            ps.setString(2, actionInfos[i].getLog());
            ps.setBoolean(3, actionInfos[i].isSuccessful());
            ps.setLong(4, actionInfos[i].getCreateTime());
            ps.setBoolean(5, actionInfos[i].isFinished());
            ps.setLong(6, actionInfos[i].getFinishTime());
            ps.setFloat(7, actionInfos[i].getProgress());
            ps.setLong(8, actionInfos[i].getActionId());
          }

          public int getBatchSize() {
            return actionInfos.length;
          }
        });
  }

  public long getMaxId() {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    Long ret = jdbcTemplate
        .queryForObject("SELECT MAX(aid) FROM " + TABLE_NAME, Long.class);
    if (ret == null) {
      return 0;
    } else {
      return ret + 1;
    }
  }

  private Map<String, Object> toMap(ActionInfo actionInfo) {
    Map<String, Object> parameters = new HashMap<>();
    parameters.put("aid", actionInfo.getActionId());
    parameters.put("cid", actionInfo.getCmdletId());
    parameters.put("action_name", actionInfo.getActionName());
    parameters.put("args", actionInfo.getArgsJsonString());
    parameters
        .put("result", StringEscapeUtils.escapeJava(actionInfo.getResult()));
    parameters.put("log", StringEscapeUtils.escapeJava(actionInfo.getLog()));
    parameters.put("successful", actionInfo.isSuccessful());
    parameters.put("create_time", actionInfo.getCreateTime());
    parameters.put("finished", actionInfo.isFinished());
    parameters.put("finish_time", actionInfo.getFinishTime());
    parameters.put("progress", actionInfo.getProgress());
    return parameters;
  }

  class ActionRowMapper implements RowMapper<ActionInfo> {

    @Override
    public ActionInfo mapRow(ResultSet resultSet, int i) throws SQLException {
      ActionInfo actionInfo = new ActionInfo();
      actionInfo.setActionId(resultSet.getLong("aid"));
      actionInfo.setCmdletId(resultSet.getLong("cid"));
      actionInfo.setActionName(resultSet.getString("action_name"));
      actionInfo.setArgsFromJsonString(resultSet.getString("args"));
      actionInfo.setResult(
          StringEscapeUtils.unescapeJava(resultSet.getString("result")));
      actionInfo
          .setLog(StringEscapeUtils.unescapeJava(resultSet.getString("log")));
      actionInfo.setSuccessful(resultSet.getBoolean("successful"));
      actionInfo.setCreateTime(resultSet.getLong("create_time"));
      actionInfo.setFinished(resultSet.getBoolean("finished"));
      actionInfo.setFinishTime(resultSet.getLong("finish_time"));
      actionInfo.setProgress(resultSet.getFloat("progress"));
      return actionInfo;
    }
  }
}
