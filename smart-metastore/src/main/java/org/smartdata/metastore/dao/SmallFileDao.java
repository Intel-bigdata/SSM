/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.smartdata.metastore.dao;

import org.smartdata.model.FileContainerInfo;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;

import javax.sql.DataSource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;


public class SmallFileDao {
  private DataSource dataSource;

  public void setDataSource(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public SmallFileDao(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public synchronized void addSmallFile(String path, FileContainerInfo fileContainerInfo) {
    SimpleJdbcInsert simpleJdbcInsert = new SimpleJdbcInsert(dataSource);
    simpleJdbcInsert.setTableName("small_file");
    Map<String, Object> parameters = new HashMap<>();
    parameters.put("path", path);
    parameters.put("container_file_path", fileContainerInfo.getContainerFilePath());
    parameters.put("offset", fileContainerInfo.getOffset());
    parameters.put("length", fileContainerInfo.getLength());
    simpleJdbcInsert.execute(parameters);
  }

  public synchronized void deleteSmallFile(String path) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String sql = "DELETE FROM small_file WHERE path = ?";
    jdbcTemplate.update(sql, path);
  }

  public FileContainerInfo getSmallFile(String path) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.queryForObject("SELECT * FROM small_file WHERE path = ?",
        new Object[]{path}, new FileContainerInfoRowMapper());
  }

  public synchronized void updateSmallFile(String path, String containerFilePath,
                                           long offset, long length) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String sql = "UPDATE small_file SET container_file_path = ? offset = ?"
        + " length = ? WHERE path = ?";
    jdbcTemplate.update(sql, containerFilePath, offset, length, path);
  }

  public int getCountByPath(String path) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    final String sql = "SELECT COUNT(*) FROM small_file WHERE path = ?";
    return jdbcTemplate.queryForObject(sql, Integer.class, path);
  }

  class FileContainerInfoRowMapper implements RowMapper<FileContainerInfo> {
    @Override
    public FileContainerInfo mapRow(ResultSet resultSet, int i)
        throws SQLException {
      return new FileContainerInfo(
          resultSet.getString("container_file_path"),
          resultSet.getLong("offset"),
          resultSet.getLong("length")
      );
    }
  }
}
