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
import org.smartdata.model.CmdletState;
import org.smartdata.model.CmdletInfo;
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

  public void setDataSource(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public CmdletDao(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public List<CmdletInfo> getAll() {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.query("select * from cmdlet",
        new CmdletRowMapper());
  }

  public List<CmdletInfo> getByIds(List<Long> aids) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.query("select * from cmdlet WHERE aid IN (?)",
        new Object[]{StringUtils.join(aids, ",")},
        new CmdletRowMapper());
  }

  public CmdletInfo getById(long cid) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.queryForObject("select * from cmdlet where cid = ?",
        new Object[]{cid}, new CmdletRowMapper());
  }

  public List<CmdletInfo> getByRid(long rid) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.query("select * from cmdlet where rid = ?",
        new Object[]{rid}, new CmdletRowMapper());
  }

  public List<CmdletInfo> getByCondition(String cidCondition,
      String ridCondition, CmdletState state) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String sqlPrefix = "SELECT * FROM cmdlet WHERE ";
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
    final String sql = "delete from cmdlet where cid = ?";
    jdbcTemplate.update(sql, cid);
  }

  public void insert(CmdletInfo CmdletInfo) {
    SimpleJdbcInsert simpleJdbcInsert = new SimpleJdbcInsert(dataSource);
    simpleJdbcInsert.setTableName("cmdlet");
    simpleJdbcInsert.execute(toMap(CmdletInfo));
  }

  public void insert(CmdletInfo[] CmdletInfos) {
    SimpleJdbcInsert simpleJdbcInsert = new SimpleJdbcInsert(dataSource);
    simpleJdbcInsert.setTableName("cmdlet");
    Map<String, Object>[] maps = new Map[CmdletInfos.length];
    for (int i = 0; i < CmdletInfos.length; i++) {
      maps[i] = toMap(CmdletInfos[i]);
    }
    simpleJdbcInsert.executeBatch(maps);
  }

  public int update(long cid, long rid, int state) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String sql = "update cmdlet set " +
        "state = ?, " +
        "state_changed_time = ? where cid = ? AND rid = ?";
    return jdbcTemplate.update(sql, state, System.currentTimeMillis(), cid, rid);
  }

  public int update(long cid, String parameters, int state) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String sql = "update cmdlet set " +
        "parameters = ?, " +
        "state = ?, " +
        "state_changed_time = ? where cid = ?";
    return jdbcTemplate.update(sql, parameters, state, System.currentTimeMillis(), cid);
  }

  public int update(final CmdletInfo CmdletInfo) {
    List<CmdletInfo> CmdletInfos = new ArrayList<>();
    CmdletInfos.add(CmdletInfo);
    return update(CmdletInfos)[0];
  }

  public int[] update(final List<CmdletInfo> CmdletInfos) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String sql = "update cmdlet set " +
        "state = ?, " +
        "state_changed_time = ? " +
        "where cid = ?";
    return jdbcTemplate.batchUpdate(sql,
        new BatchPreparedStatementSetter() {
          public void setValues(PreparedStatement ps,
              int i) throws SQLException {
            ps.setInt(1, CmdletInfos.get(i).getState().getValue());
            ps.setLong(2, CmdletInfos.get(i).getStateChangedTime());
            ps.setLong(3, CmdletInfos.get(i).getCid());
          }

          public int getBatchSize() {
            return CmdletInfos.size();
          }
        });
  }

  public long getMaxId() {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    Long ret = jdbcTemplate
        .queryForObject("select MAX(cid) from cmdlet", Long.class);
    if (ret == null) {
      return 0;
    } else {
      return ret + 1;
    }
  }

  private Map<String, Object> toMap(CmdletInfo CmdletInfo) {
    Map<String, Object> parameters = new HashMap<>();
    parameters.put("cid", CmdletInfo.getCid());
    parameters.put("rid", CmdletInfo.getRid());
    parameters.put("aids", StringUtils.join(CmdletInfo.getAidsString(), ","));
    parameters.put("state", CmdletInfo.getState().getValue());
    parameters.put("parameters", CmdletInfo.getParameters());
    parameters.put("generate_time", CmdletInfo.getGenerateTime());
    parameters.put("state_changed_time", CmdletInfo.getStateChangedTime());
    return parameters;
  }

  class CmdletRowMapper implements RowMapper<CmdletInfo> {

    @Override
    public CmdletInfo mapRow(ResultSet resultSet, int i) throws SQLException {
      CmdletInfo CmdletInfo = new CmdletInfo();
      CmdletInfo.setCid(resultSet.getLong("cid"));
      CmdletInfo.setRid(resultSet.getLong("rid"));
      CmdletInfo.setAids(convertStringListToLong(resultSet.getString("aids").split(",")));
      CmdletInfo.setState(CmdletState.fromValue((int) resultSet.getByte("state")));
      CmdletInfo.setParameters(resultSet.getString("parameters"));
      CmdletInfo.setGenerateTime(resultSet.getLong("generate_time"));
      CmdletInfo.setStateChangedTime(resultSet.getLong("state_changed_time"));
      return CmdletInfo;
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


