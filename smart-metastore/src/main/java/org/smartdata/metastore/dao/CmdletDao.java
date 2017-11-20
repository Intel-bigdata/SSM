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
import org.smartdata.model.CmdletInfo;
import org.smartdata.model.CmdletState;
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

public class CmdletDao {
  private DataSource dataSource;
  private static final String TABLE_NAME = "cmdlet";
  private final String terminiatedStates;

  public void setDataSource(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public CmdletDao(DataSource dataSource) {
    this.dataSource = dataSource;
    terminiatedStates = getTerminiatedStatesString();
  }

  public List<CmdletInfo> getAll() {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.query("SELECT * FROM " + TABLE_NAME, new CmdletRowMapper());
  }

  public List<CmdletInfo> getAPageOfCmdlet(long start, long offset,
      List<String> orderBy, List<Boolean> isDesc) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    boolean ifHasAid = false;
    StringBuilder sql =
        new StringBuilder("SELECT * FROM " + TABLE_NAME + " ORDER BY ");
    for (int i = 0; i < orderBy.size(); i++) {
      if (orderBy.get(i).equals("cid")) {
        ifHasAid = true;
      }
      sql.append(orderBy.get(i));
      if (isDesc.size() > i) {
        if (isDesc.get(i)) {
          sql.append(" desc ");
        }
        sql.append(",");
      }
    }
    if (!ifHasAid) {
      sql.append("cid,");
    }
    //delete the last char
    sql = new StringBuilder(sql.substring(0, sql.length() - 1));
    //add limit
    sql.append(" LIMIT ").append(start).append(",").append(offset).append(";");
    return jdbcTemplate.query(sql.toString(), new CmdletRowMapper());
  }

  public List<CmdletInfo> getAPageOfCmdlet(long start, long offset) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String sql = "SELECT * FROM " + TABLE_NAME + " LIMIT " + start + "," + offset + ";";
    return jdbcTemplate.query(sql, new CmdletRowMapper());
  }

  public List<CmdletInfo> getByIds(List<Long> aids) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.query(
        "SELECT * FROM " + TABLE_NAME + " WHERE aid IN (?)",
        new Object[]{StringUtils.join(aids, ",")},
        new CmdletRowMapper());
  }

  public CmdletInfo getById(long cid) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.queryForObject(
        "SELECT * FROM " + TABLE_NAME + " WHERE cid = ?",
        new Object[]{cid},
        new CmdletRowMapper());
  }

  public List<CmdletInfo> getByRid(long rid) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.query(
        "SELECT * FROM " + TABLE_NAME + " WHERE rid = ?",
        new Object[]{rid},
        new CmdletRowMapper());
  }

  public long getNumByRid(long rid) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.queryForObject(
        "SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE rid = ?",
        new Object[]{rid},
        Long.class);
  }

  public List<CmdletInfo> getByRid(long rid, long start, long offset) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String sql = "SELECT * FROM " + TABLE_NAME + " WHERE rid = " + rid
        + " LIMIT " + start + "," + offset + ";";
    return jdbcTemplate.query(sql, new CmdletRowMapper());
  }

  public List<CmdletInfo> getByRid(long rid, long start, long offset,
      List<String> orderBy, List<Boolean> isDesc) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    boolean ifHasAid = false;
    StringBuilder sql =
        new StringBuilder("SELECT * FROM " + TABLE_NAME + " WHERE rid = " + rid
            + " ORDER BY ");
    for (int i = 0; i < orderBy.size(); i++) {
      if (orderBy.get(i).equals("cid")) {
        ifHasAid = true;
      }
      sql.append(orderBy.get(i));
      if (isDesc.size() > i) {
        if (isDesc.get(i)) {
          sql.append(" desc ");
        }
        sql.append(",");
      }
    }
    if (!ifHasAid) {
      sql.append("cid,");
    }

    //delete the last char
    sql = new StringBuilder(sql.substring(0, sql.length() - 1));
    //add limit
    sql.append(" LIMIT ").append(start).append(",").append(offset).append(";");
    return jdbcTemplate.query(sql.toString(), new CmdletRowMapper());
  }

  public List<CmdletInfo> getByState(CmdletState state) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.query(
        "SELECT * FROM " + TABLE_NAME + " WHERE state = ?",
        new Object[]{state.getValue()},
        new CmdletRowMapper());
  }

  public int getNumCmdletsInTerminiatedStates() {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String query = "SELECT count(*) FROM " + TABLE_NAME
        + " WHERE state IN (" + terminiatedStates + ")";
    return jdbcTemplate.queryForObject(query, Integer.class);
  }

  public List<CmdletInfo> getByCondition(
      String cidCondition, String ridCondition, CmdletState state) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String sqlPrefix = "SELECT * FROM " + TABLE_NAME + " WHERE ";
    String sqlCid = (cidCondition == null) ? "" : "AND cid " + cidCondition;
    String sqlRid = (ridCondition == null) ? "" : "AND rid " + ridCondition;
    String sqlState = (state == null) ? "" : "AND state = " + state.getValue();
    String sqlFinal = "";
    if (cidCondition != null || ridCondition != null || state != null) {
      sqlFinal = sqlPrefix + sqlCid + sqlRid + sqlState;
      sqlFinal = sqlFinal.replaceFirst("AND ", "");
    } else {
      sqlFinal = sqlPrefix.replaceFirst("WHERE ", "");
    }
    return jdbcTemplate.query(sqlFinal, new CmdletRowMapper());
  }

  public void delete(long cid) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    final String sql = "DELETE FROM " + TABLE_NAME + " WHERE cid = ?";
    jdbcTemplate.update(sql, cid);
  }

  public int deleteBeforeTime(long timestamp) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);

    final String querysql = "SELECT cid FROM " + TABLE_NAME
        + " WHERE  generate_time < ? AND state IN (" + terminiatedStates + ")";
    List<Long> cids = jdbcTemplate.queryForList(querysql, new Object[]{timestamp}, Long.class);
    if (cids.size() == 0) {
      return 0;
    }
    final String deleteCmds = "DELETE FROM " + TABLE_NAME
        + " WHERE generate_time < ? AND state IN (" + terminiatedStates + ")";
    jdbcTemplate.update(deleteCmds, timestamp);
    final String deleteActions = "DELETE FROM action WHERE cid IN ("
        + StringUtils.join(cids, ",") + ")";
    jdbcTemplate.update(deleteActions);
    return cids.size();
  }

  public int deleteKeepNewCmd (long num) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    final String queryCids = "SELECT cid FROM " + TABLE_NAME
        + " WHERE state IN (" + terminiatedStates + ")"
        + " ORDER BY generate_time DESC LIMIT 100000 OFFSET " + num;
    List<Long> cids = jdbcTemplate.queryForList(queryCids, Long.class);
    if (cids.size() == 0) {
      return 0;
    }
    String deleteCids = StringUtils.join(cids, ",");
    final String deleteCmd = "DELETE FROM " + TABLE_NAME + " WHERE cid IN (" + deleteCids + ")";
    jdbcTemplate.update(deleteCmd);
    final String deleteActions = "DELETE FROM action WHERE cid IN (" + deleteCids + ")";
    jdbcTemplate.update(deleteActions);
    return cids.size();
  }

  public void deleteAll() {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    final String sql = "DELETE FROM " + TABLE_NAME;
    jdbcTemplate.execute(sql);
  }

  public void insert(CmdletInfo cmdletInfo) {
    SimpleJdbcInsert simpleJdbcInsert = new SimpleJdbcInsert(dataSource);
    simpleJdbcInsert.setTableName(TABLE_NAME);
    simpleJdbcInsert.execute(toMap(cmdletInfo));
  }

  public void insert(CmdletInfo[] cmdletInfos) {
    SimpleJdbcInsert simpleJdbcInsert = new SimpleJdbcInsert(dataSource);
    simpleJdbcInsert.setTableName(TABLE_NAME);
    Map<String, Object>[] maps = new Map[cmdletInfos.length];
    for (int i = 0; i < cmdletInfos.length; i++) {
      maps[i] = toMap(cmdletInfos[i]);
    }
    simpleJdbcInsert.executeBatch(maps);
  }

  public int update(long cid, long rid, int state) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String sql =
        "UPDATE " + TABLE_NAME + " SET state = ?, state_changed_time = ? WHERE cid = ? AND rid = ?";
    return jdbcTemplate.update(sql, state, System.currentTimeMillis(), cid, rid);
  }

  public int update(long cid, String parameters, int state) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String sql =
        "UPDATE "
            + TABLE_NAME
            + " SET parameters = ?, state = ?, state_changed_time = ? WHERE cid = ?";
    return jdbcTemplate.update(sql, parameters, state, System.currentTimeMillis(), cid);
  }

  public int update(final CmdletInfo cmdletInfo) {
    List<CmdletInfo> cmdletInfos = new ArrayList<>();
    cmdletInfos.add(cmdletInfo);
    return update(cmdletInfos)[0];
  }

  public int[] update(final List<CmdletInfo> cmdletInfos) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String sql = "UPDATE " + TABLE_NAME + " SET  state = ?, state_changed_time = ? WHERE cid = ?";
    return jdbcTemplate.batchUpdate(
        sql,
        new BatchPreparedStatementSetter() {
          public void setValues(PreparedStatement ps, int i) throws SQLException {
            ps.setInt(1, cmdletInfos.get(i).getState().getValue());
            ps.setLong(2, cmdletInfos.get(i).getStateChangedTime());
            ps.setLong(3, cmdletInfos.get(i).getCid());
          }

          public int getBatchSize() {
            return cmdletInfos.size();
          }
        });
  }

  public long getMaxId() {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    Long ret = jdbcTemplate.queryForObject("SELECT MAX(cid) FROM " + TABLE_NAME, Long.class);
    if (ret == null) {
      return 0;
    } else {
      return ret + 1;
    }
  }

  private Map<String, Object> toMap(CmdletInfo cmdletInfo) {
    Map<String, Object> parameters = new HashMap<>();
    parameters.put("cid", cmdletInfo.getCid());
    parameters.put("rid", cmdletInfo.getRid());
    parameters.put("aids", StringUtils.join(cmdletInfo.getAidsString(), ","));
    parameters.put("state", cmdletInfo.getState().getValue());
    parameters.put("parameters", cmdletInfo.getParameters());
    parameters.put("generate_time", cmdletInfo.getGenerateTime());
    parameters.put("state_changed_time", cmdletInfo.getStateChangedTime());
    return parameters;
  }

  private String getTerminiatedStatesString() {
    String finishedState = "";
    for (CmdletState cmdletState : CmdletState.getTerminalStates()) {
      finishedState = finishedState + cmdletState.getValue() + ",";
    }
    return finishedState.substring(0, finishedState.length() - 1);
  }

  class CmdletRowMapper implements RowMapper<CmdletInfo> {

    @Override
    public CmdletInfo mapRow(ResultSet resultSet, int i) throws SQLException {
      CmdletInfo cmdletInfo = new CmdletInfo();
      cmdletInfo.setCid(resultSet.getLong("cid"));
      cmdletInfo.setRid(resultSet.getLong("rid"));
      cmdletInfo.setAids(convertStringListToLong(resultSet.getString("aids").split(",")));
      cmdletInfo.setState(CmdletState.fromValue((int) resultSet.getByte("state")));
      cmdletInfo.setParameters(resultSet.getString("parameters"));
      cmdletInfo.setGenerateTime(resultSet.getLong("generate_time"));
      cmdletInfo.setStateChangedTime(resultSet.getLong("state_changed_time"));
      return cmdletInfo;
    }

    private List<Long> convertStringListToLong(String[] strings) {
      List<Long> ret = new ArrayList<>();
      try {
        for (String s : strings) {
          ret.add(Long.valueOf(s));
        }
      } catch (NumberFormatException e) {
        // Return empty
        ret.clear();
      }
      return ret;
    }
  }
}
