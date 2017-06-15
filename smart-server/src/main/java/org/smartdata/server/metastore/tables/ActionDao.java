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
package org.smartdata.server.metastore.tables;

import org.apache.commons.lang.StringUtils;
import org.smartdata.common.actions.ActionInfo;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSourceUtils;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ActionDao {

  private JdbcTemplate jdbcTemplate;
  private SimpleJdbcInsert simpleJdbcInsert;

  public ActionDao(DataSource dataSource) {
    jdbcTemplate = new JdbcTemplate(dataSource);
    simpleJdbcInsert = new SimpleJdbcInsert(dataSource);
    simpleJdbcInsert.setTableName("actions");
  }

  public List<ActionInfo> getAllAction() {
    return jdbcTemplate.query("select * from actions",
        new ActionRowMapper());
  }

  public ActionInfo getActionById(long aid) {
    return jdbcTemplate.queryForObject("select * from actions where aid = ?",
        new Object[]{aid}, new ActionRowMapper());
  }

  public List<ActionInfo> getActionsByIds(List<Long> aids) {
    return jdbcTemplate.query("select * from actions WHERE aid IN (?)",
        new Object[]{StringUtils.join(aids, ",")},
        new ActionRowMapper());
  }

  public List<ActionInfo> getActionsByCid(long cid) {
    return jdbcTemplate.query("select * from actions where cid = ?",
        new Object[]{cid}, new ActionRowMapper());
  }

  public List<ActionInfo> getLatestActions(int size) {
    String sql = "select * from actions WHERE finished = 1" +
        " ORDER by create_time DESC limit ?";
    return jdbcTemplate.query(sql, new Object[]{size},
        new ActionRowMapper());
  }

  public void delete(long aid) {
    final String sql = "delete from actions where aid = ?";
    jdbcTemplate.update(sql, aid);
  }

  public void insert(ActionInfo actionInfo) {
    simpleJdbcInsert.execute(toMap(actionInfo));
  }

  public void insert(ActionInfo[] actionInfos) {
    SqlParameterSource[] batch = SqlParameterSourceUtils
        .createBatch(actionInfos);
    simpleJdbcInsert.executeBatch(batch);
  }

  public int update(final ActionInfo actionInfo) {
    List<ActionInfo> actionInfos = new ArrayList<>();
    actionInfos.add(actionInfo);
    return batchUpdate(actionInfos)[0];
  }

  public long getMaxId() {
    Long ret = this.jdbcTemplate
        .queryForObject("select MAX(aid) from actions", Long.class);
    if (ret == null) {
      return 0;
    } else {
      return ret + 1;
    }
  }

  public int[] batchUpdate(final List<ActionInfo> actionInfos) {
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
          public void setValues(PreparedStatement ps, int i) throws SQLException {
            ps.setString(1, actionInfos.get(i).getResult());
            ps.setString(2, actionInfos.get(i).getLog());
            ps.setBoolean(3, actionInfos.get(i).isSuccessful());
            ps.setLong(4, actionInfos.get(i).getCreateTime());
            ps.setBoolean(5, actionInfos.get(i).isFinished());
            ps.setLong(6, actionInfos.get(i).getFinishTime());
            ps.setFloat(7, actionInfos.get(i).getProgress());
            ps.setLong(8, actionInfos.get(i).getActionId());
          }

          public int getBatchSize() {
            return actionInfos.size();
          }
        });
  }

  private Map<String, Object> toMap(ActionInfo actionInfo) {
    Map<String, Object> parameters = new HashMap<>();
    parameters.put("aid", actionInfo.getActionId());
    parameters.put("cid", actionInfo.getCommandId());
    parameters.put("action_name", actionInfo.getActionName());
    parameters.put("args", actionInfo.getArgs());
    parameters.put("result", actionInfo.getResult());
    parameters.put("log", actionInfo.getLog());
    parameters.put("successful", actionInfo.isSuccessful());
    parameters.put("create_time", actionInfo.getCreateTime());
    parameters.put("finished", actionInfo.isFinished());
    parameters.put("finish_time", actionInfo.getFinishTime());
    parameters.put("progress", actionInfo.getProgress());
    return parameters;
  }

}


class ActionRowMapper implements RowMapper<ActionInfo> {

  @Override
  public ActionInfo mapRow(ResultSet resultSet, int i) throws SQLException {
    ActionInfo actionInfo = new ActionInfo();
    actionInfo.setActionId(resultSet.getLong("aid"));
    actionInfo.setCommandId(resultSet.getLong("cid"));
    actionInfo.setActionName(resultSet.getString("action_name"));
    actionInfo.setArgs(resultSet.getString("args").split(","));
    actionInfo.setResult(resultSet.getString("result"));
    actionInfo.setLog(resultSet.getString("log"));
    actionInfo.setSuccessful(resultSet.getBoolean("successful"));
    actionInfo.setCreateTime(resultSet.getLong("create_time"));
    actionInfo.setFinished(resultSet.getBoolean("finished"));
    actionInfo.setFinishTime(resultSet.getLong("finish_time"));
    actionInfo.setProgress(resultSet.getFloat("progress"));
    return actionInfo;
  }
}