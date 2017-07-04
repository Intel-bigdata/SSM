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

  private DataSource dataSource;

  public void setDataSource(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public ActionDao(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public List<ActionInfo> getAll() {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.query("select * from actions",
        new ActionRowMapper());
  }

  public ActionInfo getById(long aid) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.queryForObject("select * from actions where aid = ?",
        new Object[]{aid}, new ActionRowMapper());
  }

  public List<ActionInfo> getByIds(List<Long> aids) {
    NamedParameterJdbcTemplate namedParameterJdbcTemplate =
        new NamedParameterJdbcTemplate(dataSource);
    MapSqlParameterSource parameterSource = new MapSqlParameterSource();
    parameterSource.addValue("aids", aids);
    return namedParameterJdbcTemplate.query("select * from actions WHERE aid IN (:aids)",
        parameterSource, new ActionRowMapper());
  }

  public List<ActionInfo> getByCid(long cid) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.query("select * from actions where cid = ?",
        new Object[]{cid}, new ActionRowMapper());
  }

  public List<ActionInfo> getByCondition(String aidCondition,
      String cidCondition) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String sqlPrefix = "SELECT * FROM actions WHERE ";
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
    jdbcTemplate.setMaxRows(size);
    String sql = "select * from actions WHERE finished = 1" +
        " ORDER by create_time DESC";
    return jdbcTemplate.query(sql, new ActionRowMapper());
  }

  public void delete(long aid) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    final String sql = "delete from actions where aid = ?";
    jdbcTemplate.update(sql, aid);
  }

  public void insert(ActionInfo actionInfo) {
    SimpleJdbcInsert simpleJdbcInsert = new SimpleJdbcInsert(dataSource);
    simpleJdbcInsert.setTableName("actions");
    simpleJdbcInsert.execute(toMap(actionInfo));
  }

  public void insert(ActionInfo[] actionInfos) {
    // TODO need upgrade
    // SqlParameterSource[] batch = SqlParameterSourceUtils
    //     .createBatch(actionInfos);
    // simpleJdbcInsert.executeBatch(batch);


//    for (ActionInfo actionInfo : actionInfos) {
//      insert(actionInfo);
//    }

    //A new way to batch insert
    SimpleJdbcInsert simpleJdbcInsert = new SimpleJdbcInsert(dataSource);
    simpleJdbcInsert.setTableName("actions");

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
    String sql = "update actions set " +
        "result = ?, " +
        "log = ?, " +
        "successful = ?, " +
        "create_time = ?, " +
        "finished = ?, " +
        "finish_time = ?, " +
        "progress = ? " +
        "where aid = ?";
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
        .queryForObject("select MAX(aid) from actions", Long.class);
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
    parameters.put("result", actionInfo.getResult());
    parameters.put("log", actionInfo.getLog());
    parameters.put("successful", actionInfo.isSuccessful());
    parameters.put("create_time", actionInfo.getCreateTime());
    parameters.put("finished", actionInfo.isFinished());
    parameters.put("finish_time", actionInfo.getFinishTime());
    parameters.put("progress", (int)(actionInfo.getProgress()));
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
      actionInfo.setResult(resultSet.getString("result"));
      actionInfo.setLog(resultSet.getString("log"));
      actionInfo.setSuccessful(resultSet.getBoolean("successful"));
      actionInfo.setCreateTime(resultSet.getLong("create_time"));
      actionInfo.setFinished(resultSet.getBoolean("finished"));
      actionInfo.setFinishTime(resultSet.getLong("finish_time"));
      actionInfo.setProgress(resultSet.getInt("progress"));
      return actionInfo;
    }
  }

}
