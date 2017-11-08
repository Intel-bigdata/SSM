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

import org.smartdata.model.RuleInfo;
import org.smartdata.model.RuleState;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;

import javax.sql.DataSource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RuleDao {

  private DataSource dataSource;

  public void setDataSource(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public RuleDao(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public List<RuleInfo> getAll() {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.query("SELECT * FROM rule",
        new RuleRowMapper());
  }

  public RuleInfo getById(long id) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.queryForObject("SELECT * FROM rule WHERE id = ?",
        new Object[]{id}, new RuleRowMapper());
  }

  public List<RuleInfo> getAPageOfRule(long start, long offset, List<String> orderBy,
      List<Boolean> isDesc) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    boolean ifHasAid = false;
    String sql = "SELECT * FROM rule ORDER BY ";

    for (int i = 0; i < orderBy.size(); i++) {
      if (orderBy.get(i).equals("rid")) {
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
      sql = sql + "rid,";
    }

    sql = sql.substring(0, sql.length() - 1);
    sql = sql + " LIMIT " + start + "," + offset + ";";
    return jdbcTemplate.query(sql, new RuleRowMapper());
  }

  public List<RuleInfo> getAPageOfRule(long start, long offset) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String sql = "SELECT * FROM rule LIMIT " + start + "," + offset + ";";
    return jdbcTemplate.query(sql, new RuleRowMapper());
  }

  public long insert(RuleInfo ruleInfo) {
    SimpleJdbcInsert simpleJdbcInsert = new SimpleJdbcInsert(dataSource);
    simpleJdbcInsert.setTableName("rule");
    simpleJdbcInsert.usingGeneratedKeyColumns("id");
    long id = simpleJdbcInsert.executeAndReturnKey(toMap(ruleInfo)).longValue();
    ruleInfo.setId(id);
    return id;
  }

  public int update(long ruleId, long lastCheckTime, long checkedCount, int cmdletsGen) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String sql =
        "UPDATE rule SET last_check_time = ?, checked_count = ?, "
            + "generated_cmdlets = ? WHERE id = ?";
    return jdbcTemplate.update(sql, lastCheckTime, checkedCount, cmdletsGen, ruleId);
  }

  public int update(long ruleId, int rs, long lastCheckTime, long checkedCount, int cmdletsGen) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String sql =
        "UPDATE rule SET state = ?, last_check_time = ?, checked_count = ?, "
            + "generated_cmdlets = ? WHERE id = ?";
    return jdbcTemplate.update(sql, rs, lastCheckTime, checkedCount, cmdletsGen, ruleId);
  }

  public int update(long ruleId, int rs) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String sql = "UPDATE rule SET state = ? WHERE id = ?";
    return jdbcTemplate.update(sql, rs, ruleId);
  }

  public void delete(long id) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    final String sql = "DELETE FROM rule WHERE id = ?";
    jdbcTemplate.update(sql, id);
  }

  public void deleteAll() {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    final String sql = "DELETE FROM rule";
    jdbcTemplate.update(sql);
  }

  private Map<String, Object> toMap(RuleInfo ruleInfo) {
    Map<String, Object> parameters = new HashMap<>();
    if (ruleInfo.getSubmitTime() == 0) {
      ruleInfo.setSubmitTime(System.currentTimeMillis());
    }
    parameters.put("submit_time", ruleInfo.getSubmitTime());
    parameters.put("rule_text", ruleInfo.getRuleText());
    parameters.put("state", ruleInfo.getState().getValue());
    parameters.put("checked_count", ruleInfo.getNumChecked());
    parameters.put("generated_cmdlets", ruleInfo.getNumCmdsGen());
    parameters.put("last_check_time", ruleInfo.getLastCheckTime());
    return parameters;
  }

  class RuleRowMapper implements RowMapper<RuleInfo> {

    @Override
    public RuleInfo mapRow(ResultSet resultSet, int i) throws SQLException {
      RuleInfo ruleInfo = new RuleInfo();
      ruleInfo.setId(resultSet.getLong("id"));
      ruleInfo.setSubmitTime(resultSet.getLong("submit_time"));
      ruleInfo.setRuleText(resultSet.getString("rule_text"));
      ruleInfo.setState(RuleState.fromValue((int) resultSet.getByte("state")));
      ruleInfo.setNumChecked(resultSet.getLong("checked_count"));
      ruleInfo.setNumCmdsGen(resultSet.getLong("generated_cmdlets"));
      ruleInfo.setLastCheckTime(resultSet.getLong("last_check_time"));
      return ruleInfo;
    }
  }
}
