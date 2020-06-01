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

import org.smartdata.model.UserInfo;
import org.smartdata.utils.StringUtil;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;

import javax.sql.DataSource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UserInfoDao {
  private static final String TABLE_NAME = "user_info";
  private DataSource dataSource;

  public void setDataSource(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public UserInfoDao(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public List<UserInfo> getAll() {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.query("SELECT * FROM " + TABLE_NAME, new UserInfoRowMapper());
  }

  public boolean containsUserName(String name) {
    return !list(name).isEmpty();
  }

  private List<UserInfo> list(String name) {
    return new JdbcTemplate(dataSource)
        .query(
            "SELECT * FROM " + TABLE_NAME + " WHERE user_name = ?",
            new Object[] {name},
            new UserInfoRowMapper());
  }

  public UserInfo getByUserName(String name) {
    List<UserInfo> infos = list(name);
    return infos.isEmpty() ? null : infos.get(0);
  }

  public void delete(String name) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    final String sql = "DELETE FROM " + TABLE_NAME + " WHERE user_name = ?";
    jdbcTemplate.update(sql, name);
  }

  public void insert(UserInfo userInfo) {
    SimpleJdbcInsert simpleJdbcInsert = new SimpleJdbcInsert(dataSource);
    simpleJdbcInsert.setTableName(TABLE_NAME);
    simpleJdbcInsert.execute(toMap(new UserInfo(userInfo.getUserName(),
        StringUtil.toSHA512String(userInfo.getUserPassword()))));
  }

  public boolean authentic (UserInfo userInfo) {
    UserInfo origin = getByUserName(userInfo.getUserName());
    return origin.equals(userInfo);
  }

  public int newPassword(UserInfo userInfo) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String sql = "UPDATE " + TABLE_NAME + " SET user_password = ? WHERE user_name = ?";
    return jdbcTemplate.update(sql, StringUtil.toSHA512String(userInfo.getUserPassword()),
        userInfo.getUserName());
  }

  public void deleteAll() {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    final String sql = "DELETE FROM " + TABLE_NAME;
    jdbcTemplate.execute(sql);
  }

  private Map<String, Object> toMap(UserInfo userInfo) {
    Map<String, Object> parameters = new HashMap<>();
    parameters.put("user_name", userInfo.getUserName());
    parameters.put("user_password", userInfo.getUserPassword());
    return parameters;
  }

  class UserInfoRowMapper implements RowMapper<UserInfo> {

    @Override
    public UserInfo mapRow(ResultSet resultSet, int i) throws SQLException {
      return new UserInfo(resultSet.getString("user_name"), resultSet.getString("user_password"));
    }
  }
}
