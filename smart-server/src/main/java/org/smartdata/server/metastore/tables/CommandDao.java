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
import org.smartdata.common.CommandState;
import org.smartdata.common.command.CommandInfo;
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

public class CommandDao {
  private JdbcTemplate jdbcTemplate;
  private SimpleJdbcInsert simpleJdbcInsert;

  public CommandDao(DataSource dataSource) {
    this.jdbcTemplate = new JdbcTemplate(dataSource);
    this.simpleJdbcInsert = new SimpleJdbcInsert(dataSource);
    simpleJdbcInsert.withTableName("commands");
  }

  public List<CommandInfo> getAllCommand() {
    return jdbcTemplate.query("select * from commands",
        new CommandRowMapper());
  }

  public List<CommandInfo> getCommandsByIds(List<Long> aids) {
    return jdbcTemplate.query("select * from commands WHERE aid IN (?)",
        new Object[]{StringUtils.join(aids, ",")},
        new CommandRowMapper());
  }

  public CommandInfo getCommandById(long cid) {
    return jdbcTemplate.queryForObject("select * from commands where cid = ?",
        new Object[]{cid}, new CommandRowMapper());
  }

  public List<CommandInfo> getCommandsByRid(long rid) {
    return jdbcTemplate.query("select * from commands where rid = ?",
        new Object[]{rid}, new CommandRowMapper());
  }

  public void delete(long cid) {
    final String sql = "delete from commands where cid = ?";
    jdbcTemplate.update(sql, cid);
  }

  public void insert(CommandInfo CommandInfo) {
    simpleJdbcInsert.execute(toMap(CommandInfo));
  }

  public void insert(CommandInfo[] CommandInfos) {
    SqlParameterSource[] batch = SqlParameterSourceUtils
        .createBatch(CommandInfos);
    simpleJdbcInsert.executeBatch(batch);
  }

  public int update(final CommandInfo CommandInfo) {
    List<CommandInfo> CommandInfos = new ArrayList<>();
    CommandInfos.add(CommandInfo);
    return batchUpdate(CommandInfos)[0];
  }

  public int[] batchUpdate(final List<CommandInfo> CommandInfos) {
    String sql = "update commands set " +
        "state = ?, " +
        "state_changed_time = ? " +
        "where cid = ?";
    return jdbcTemplate.batchUpdate(sql,
        new BatchPreparedStatementSetter() {
          public void setValues(PreparedStatement ps, int i) throws SQLException {
            ps.setInt(1, CommandInfos.get(i).getState().getValue());
            ps.setLong(2, CommandInfos.get(i).getStateChangedTime());
            ps.setLong(3, CommandInfos.get(i).getCid());
          }

          public int getBatchSize() {
            return CommandInfos.size();
          }
        });
  }

  public long getMaxId() {
    Long ret = this.jdbcTemplate
        .queryForObject("select MAX(cid) from commands", Long.class);
    if (ret == null) {
      return 0;
    } else {
      return ret + 1;
    }
  }

  private Map<String, Object> toMap(CommandInfo CommandInfo) {
    Map<String, Object> parameters = new HashMap<>();
    parameters.put("cid", CommandInfo.getCid());
    parameters.put("rid", CommandInfo.getRid());
    parameters.put("aids", CommandInfo.getAids());
    parameters.put("state", CommandInfo.getState());
    parameters.put("parameters", CommandInfo.getParameters());
    parameters.put("generate_time", CommandInfo.getGenerateTime());
    parameters.put("state_changed_time", CommandInfo.getStateChangedTime());
    return parameters;
  }

  class CommandRowMapper implements RowMapper<CommandInfo> {

    @Override
    public CommandInfo mapRow(ResultSet resultSet, int i) throws SQLException {
      CommandInfo CommandInfo = new CommandInfo();
      CommandInfo.setCid(resultSet.getLong("cid"));
      CommandInfo.setRid(resultSet.getLong("rid"));
      CommandInfo.setAids(convertStringListToLong(resultSet.getString("aids").split(",")));
      CommandInfo.setState(CommandState.fromValue((int) resultSet.getByte("state")));
      CommandInfo.setParameters(resultSet.getString("parameters"));
      CommandInfo.setGenerateTime(resultSet.getLong("generate_time"));
      CommandInfo.setStateChangedTime(resultSet.getLong("state_changed_time"));
      return CommandInfo;
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


