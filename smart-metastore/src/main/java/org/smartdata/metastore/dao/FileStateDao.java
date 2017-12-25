/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.smartdata.metastore.dao;

import org.smartdata.model.FileState;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import javax.sql.DataSource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FileStateDao {
  private static final String TABLE_NAME = "file_state";
  private DataSource dataSource;

  public void setDataSource(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public FileStateDao(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public void insertUpate(FileState fileState) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String sql = "REPLACE INTO " + TABLE_NAME + " (path, type, stage) VALUES (?,?,?);";
    jdbcTemplate.update(sql, fileState.getPath(), fileState.getFileType().getValue(),
        fileState.getFileStage().getValue());
  }

  public FileState getByPath(String path) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.queryForObject("SELECT * FROM " + TABLE_NAME + " WHERE path = ?",
        new Object[]{path}, new FileStateRowMapper());
  }

  public List<FileState> getAll() {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.query("SELECT * FROM " + TABLE_NAME,
        new FileStateRowMapper());
  }

  public void deleteByPath(String path) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    final String sql = "DELETE FROM " + TABLE_NAME + " WHERE path = ?";
    jdbcTemplate.update(sql, path);
  }

  public void deleteAll() {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    final String sql = "DELETE FROM " + TABLE_NAME;
    jdbcTemplate.execute(sql);
  }

  private Map<String, Object> toMap(FileState fileState) {
    Map<String, Object> parameters = new HashMap<>();
    parameters.put("path", fileState.getPath());
    parameters.put("type", fileState.getFileType().getValue());
    parameters.put("stage", fileState.getFileStage().getValue());
    return parameters;
  }

  class FileStateRowMapper implements RowMapper<FileState> {
    @Override
    public FileState mapRow(ResultSet resultSet, int i)
        throws SQLException {
      FileState fileState = new FileState(resultSet.getString("path"),
          FileState.FileType.fromValue(resultSet.getInt("type")),
          FileState.FileStage.fromValue(resultSet.getInt("stage")));
      return fileState;
    }
  }
}
