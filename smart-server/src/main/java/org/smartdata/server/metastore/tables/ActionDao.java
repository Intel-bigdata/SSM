package org.smartdata.server.metastore.tables;

import org.smartdata.common.actions.ActionInfo;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ActionDao {

  private JdbcTemplate jdbcTemplate;
  private SimpleJdbcInsert simpleJdbcInsert;

  public void init(DataSource dataSource) {
    this.jdbcTemplate = new JdbcTemplate(dataSource);
  }

  @Transactional(readOnly = true)
  public List<ActionInfo> getAllAction() {
    return jdbcTemplate.query("select * from actions", new ActionRowMapper());
  }

  @Transactional(readOnly = true)
  public ActionInfo getActionById(long aid) {
    return jdbcTemplate.queryForObject("select * from actions where aid = ?",
        new Object[]{aid}, new ActionRowMapper());
  }

  @Transactional(readOnly = true)
  public ActionInfo getActionByParameter(long aid) {
    return jdbcTemplate.queryForObject("select * from actions where aid = ?",
        new Object[]{aid}, new ActionRowMapper());
  }

  @Transactional(readOnly = true)
  public List<ActionInfo> getLatestActions(int size) {
    String sql = "select * from actions WHERE finished = 1" +
        " ORDER by create_time DESC limit ?";
    return jdbcTemplate.query(sql, new Object[]{size},
        new ActionRowMapper());
  }

  @Transactional
  public void delete(long aid) {
    final String sql = "delete from actions where aid = ?";
    jdbcTemplate.update(sql, new Object[] {aid}, new long[] {Types.BIGINT});
  }

  public void insert(ActionInfo actionInfo) {
    simpleJdbcInsert.withTableName("actions");
    simpleJdbcInsert.execute(toMap(actionInfo));
  }

  public int update(final ActionInfo actionInfo) {
    List<ActionInfo> actionInfos = new ArrayList<>();
    actionInfos.add(actionInfo);
    return batchUpdate(actionInfos)[0];
  }

  public int[] batchUpdate(final List<ActionInfo> actionInfos) {
    String sql = "update actions set result = ?, " +
        "log = ?, successful = ?, create_time = ?, finished = ?, " +
        "finish_time = ?, progress = ?, where aid = ?";
    return jdbcTemplate.batchUpdate(sql,
        new ActionStatementSetter(actionInfos));
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

class ActionStatementSetter implements BatchPreparedStatementSetter {

  private List<ActionInfo> actionInfos;

  public ActionStatementSetter(List<ActionInfo> actionInfos) {
    this.actionInfos = actionInfos;
  }

  @Override
  public void setValues(PreparedStatement preparedStatement, int i) throws SQLException {
    preparedStatement.setString(1, actionInfos.get(i).getResult());
    preparedStatement.setString(2, actionInfos.get(i).getLog());
    preparedStatement.setBoolean(3, actionInfos.get(i).isSuccessful());
    preparedStatement.setLong(4, actionInfos.get(i).getCreateTime());
    preparedStatement.setBoolean(5, actionInfos.get(i).isFinished());
    preparedStatement.setLong(6, actionInfos.get(i).getFinishTime());
    preparedStatement.setFloat(7, actionInfos.get(i).getProgress());
    preparedStatement.setLong(8, actionInfos.get(i).getActionId());
  }

  @Override
  public int getBatchSize() {
    return actionInfos.size();
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