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

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import javax.sql.DataSource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GroupsDao {
  private DataSource dataSource;

  public void setDataSource(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public GroupsDao(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public synchronized void addGroup(String groupName) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String sql = String.format(
        "INSERT INTO user_group (group_name) VALUES ('%s')", groupName);
    jdbcTemplate.execute(sql);
  }

  public synchronized void deleteGroup(String groupName) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String sql = String.format(
        "DELETE FROM user_group WHERE group_name = '%s'", groupName);
    jdbcTemplate.execute(sql);
  }

  public int getCountGroups() {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.queryForObject(
        "SELECT COUNT(*) FROM user_group", Integer.class);
  }

  public List<String> listGroup() {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    List<String> groups = jdbcTemplate.query(
        "SELECT group_name FROM user_group",
        new RowMapper<String>() {
          public String mapRow(ResultSet rs, int rowNum) throws SQLException {
            return rs.getString("group_name");
          }
        });
    return groups;
  }

  public Map<Integer, String> getGroupsMap() {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return toMap(jdbcTemplate.queryForList("SELECT * FROM user_group"));
  }

  private Map<Integer, String> toMap(List<Map<String, Object>> list) {
    Map<Integer, String> res = new HashMap<>();
    for (Map<String, Object> map : list) {
      res.put((Integer) map.get("gid"), (String) map.get("group_name"));
    }
    return res;
  }
}
