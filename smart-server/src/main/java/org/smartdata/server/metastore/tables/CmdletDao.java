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
import org.smartdata.common.CmdletState;
import org.smartdata.common.cmdlet.CmdletInfo;
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

public class CmdletDao {
  private JdbcTemplate jdbcTemplate;
  private SimpleJdbcInsert simpleJdbcInsert;

  public CmdletDao(DataSource dataSource) {
    this.jdbcTemplate = new JdbcTemplate(dataSource);
    this.simpleJdbcInsert = new SimpleJdbcInsert(dataSource);
    simpleJdbcInsert.withTableName("cmdlets");
  }

  public List<CmdletInfo> getAll() {
    return jdbcTemplate.query("select * from cmdlets",
        new CmdletRowMapper());
  }

  public List<CmdletInfo> getByIds(List<Long> aids) {
    return jdbcTemplate.query("select * from cmdlets WHERE aid IN (?)",
        new Object[]{StringUtils.join(aids, ",")},
        new CmdletRowMapper());
  }

  public CmdletInfo getById(long cid) {
    return jdbcTemplate.queryForObject("select * from cmdlets where cid = ?",
        new Object[]{cid}, new CmdletRowMapper());
  }

  public List<CmdletInfo> getCmdletsByRid(long rid) {
    return jdbcTemplate.query("select * from cmdlets where rid = ?",
        new Object[]{rid}, new CmdletRowMapper());
  }

  public void delete(long cid) {
    final String sql = "delete from cmdlets where cid = ?";
    jdbcTemplate.update(sql, cid);
  }

  public void insert(CmdletInfo CmdletInfo) {
    simpleJdbcInsert.execute(toMap(CmdletInfo));
  }

  public void insert(CmdletInfo[] CmdletInfos) {
    SqlParameterSource[] batch = SqlParameterSourceUtils
        .createBatch(CmdletInfos);
    simpleJdbcInsert.executeBatch(batch);
  }

  public int update(final CmdletInfo CmdletInfo) {
    List<CmdletInfo> CmdletInfos = new ArrayList<>();
    CmdletInfos.add(CmdletInfo);
    return update(CmdletInfos)[0];
  }

  public int[] update(final List<CmdletInfo> CmdletInfos) {
    String sql = "update cmdlets set " +
        "state = ?, " +
        "state_changed_time = ? " +
        "where cid = ?";
    return jdbcTemplate.batchUpdate(sql,
        new BatchPreparedStatementSetter() {
          public void setValues(PreparedStatement ps, int i) throws SQLException {
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
    Long ret = this.jdbcTemplate
        .queryForObject("select MAX(cid) from cmdlets", Long.class);
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
    parameters.put("aids", CmdletInfo.getAids());
    parameters.put("state", CmdletInfo.getState());
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


