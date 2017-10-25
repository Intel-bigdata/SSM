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

public class UserDao {

  private DataSource dataSource;

  public void setDataSource(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public UserDao(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public synchronized void addUser(String userName) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String sql = String.format("INSERT INTO owner (owner_name) VALUES ('%s')", userName);
    jdbcTemplate.execute(sql);
  }

  public synchronized void deleteUser(String user) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String sql = String.format(
        "DELETE FROM owner WHERE owner_name = '%s'", user);
    jdbcTemplate.execute(sql);
  }

  public List<String> listUser() {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    List<String> user = jdbcTemplate.query(
        "SELECT owner_name FROM owner",
        new RowMapper<String>() {
          public String mapRow(ResultSet rs, int rowNum) throws SQLException {
            return rs.getString("owner_name");
          }
        });
    return user;
  }

  public int getCountUsers() {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.queryForObject(
        "SELECT COUNT(*) FROM owner", Integer.class);
  }

  public Map<Integer, String> getUsersMap() {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return toMap(jdbcTemplate.queryForList("SELECT * FROM owner"));
  }

  private Map<Integer, String> toMap(List<Map<String, Object>> list) {
    Map<Integer, String> res = new HashMap<>();
    for (Map<String, Object> map : list) {
      res.put((Integer) map.get("oid"), (String) map.get("owner_name"));
    }
    return res;
  }
}
