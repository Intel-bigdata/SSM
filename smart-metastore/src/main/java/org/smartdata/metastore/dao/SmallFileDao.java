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

import org.smartdata.model.CompactFileState;
import org.smartdata.model.FileContainerInfo;
import org.smartdata.model.FileState;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import javax.sql.DataSource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class SmallFileDao {
  private DataSource dataSource;

  public void setDataSource(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public SmallFileDao(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public void insertUpdate(String path, FileContainerInfo fileContainerInfo) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String sql = "REPLACE INTO small_file (path, container_file_path, offset, length)"
        + " VALUES (?,?,?,?)";
    jdbcTemplate.update(sql, path, fileContainerInfo.getContainerFilePath(),
        fileContainerInfo.getOffset(), fileContainerInfo.getLength());
  }

  public void deleteByPath(String path, boolean recursive) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String sql = "DELETE FROM small_file WHERE path = ?";
    jdbcTemplate.update(sql, path);
    if (recursive) {
      sql = "DELETE FROM small_file WHERE path LIKE ?";
      jdbcTemplate.update(sql, path + "/%");
    }
  }

  public FileState getFileStateByPath(String path) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.queryForObject("SELECT * FROM small_file WHERE path = ?",
        new Object[]{path}, new FileStateRowMapper());
  }

  public List<FileState> getFileStatesByContainerFilePath(String containerFilePath) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.query("SELECT * FROM small_file WHERE container_file_path = ?",
        new FileStateRowMapper(), containerFilePath);
  }

  private class FileStateRowMapper implements RowMapper<FileState> {
    @Override
    public FileState mapRow(ResultSet resultSet, int i)
        throws SQLException {
      return new CompactFileState(resultSet.getString("path"),
          new FileContainerInfo(
              resultSet.getString("container_file_path"),
              resultSet.getLong("offset"),
              resultSet.getLong("length"))
      );
    }
  }
}
