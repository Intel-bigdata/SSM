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

import org.smartdata.common.rule.RuleInfo;
import org.smartdata.common.rule.RuleState;
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

  private JdbcTemplate jdbcTemplate;
  private SimpleJdbcInsert simpleJdbcInsert;

  public RuleDao(DataSource dataSource) {
    jdbcTemplate = new JdbcTemplate(dataSource);
    simpleJdbcInsert = new SimpleJdbcInsert(dataSource);
    simpleJdbcInsert.setTableName("rules");
  }

  public List<RuleInfo> getAll() {
    return jdbcTemplate.query("select * from rules",
        new RuleRowMapper());
  }

  public RuleInfo getById(long id) {
    return jdbcTemplate.queryForObject("select * from rules where id = ?",
        new Object[]{id}, new RuleRowMapper());
  }


  public int insert(RuleInfo ruleInfo) {
    return simpleJdbcInsert.execute(toMap(ruleInfo));
  }

  public void update(long ruleId, RuleState rs,
                     long lastCheckTime, long checkedCount, int commandsGen) {
    String sql = "update rules set " +
        "state = ?, " +
        "last_check_time = ?, " +
        "checked_count = ?, " +
        "commands_generated = ? where id = ?";
    jdbcTemplate.update(sql, rs.getValue(), lastCheckTime, checkedCount, commandsGen, ruleId);
  }

  public void delete(long id) {
    final String sql = "delete from rules where id = ?";
    jdbcTemplate.update(sql, id);
  }

  public void deleteAll() {
    final String sql = "delete from rules";
    jdbcTemplate.update(sql);
  }

  private Map<String, Object> toMap(RuleInfo ruleInfo) {
    Map<String, Object> parameters = new HashMap<>();
    parameters.put("id", ruleInfo.getId());
    parameters.put("submit_time", ruleInfo.getSubmitTime());
    parameters.put("rule_text", ruleInfo.getRuleText());
    parameters.put("state", ruleInfo.getState().getValue());
    parameters.put("checked_count", ruleInfo.getNumChecked());
    parameters.put("commands_generated", ruleInfo.getNumCmdsGen());
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
      ruleInfo.setNumCmdsGen(resultSet.getLong("commands_generated"));
      ruleInfo.setLastCheckTime(resultSet.getLong("last_check_time"));
      return ruleInfo;
    }
  }
}
