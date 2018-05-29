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
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import javax.sql.DataSource;

import java.sql.PreparedStatement;
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

  public void insertUpdate(FileState fileState) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String sql = "REPLACE INTO " + TABLE_NAME + " (path, type, stage) VALUES (?,?,?)";
    jdbcTemplate.update(sql, fileState.getPath(), fileState.getFileType().getValue(),
        fileState.getFileStage().getValue());
  }

  public int[] batchInsertUpdate(final FileState[] fileStates) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String sql = "REPLACE INTO " + TABLE_NAME + " (path, type, stage) VALUES (?,?,?)";
    return jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
      @Override
      public void setValues(PreparedStatement ps,
          int i) throws SQLException {
        ps.setString(1, fileStates[i].getPath());
        ps.setInt(2, fileStates[i].getFileType().getValue());
        ps.setInt(3, fileStates[i].getFileStage().getValue());
      }
      @Override
      public int getBatchSize() {
        return fileStates.length;
      }
    });
  }

  public FileState getByPath(String path) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.queryForObject("SELECT * FROM " + TABLE_NAME + " WHERE path = ?",
        new Object[]{path}, new FileStateRowMapper());
  }

  public Map<String, FileState> getByPaths(List<String> paths) {
    NamedParameterJdbcTemplate namedParameterJdbcTemplate =
        new NamedParameterJdbcTemplate(dataSource);
    Map<String, FileState> fileStateMap = new HashMap<>();
    MapSqlParameterSource parameterSource = new MapSqlParameterSource();
    parameterSource.addValue("paths", paths);
    List<FileState> fileStates = namedParameterJdbcTemplate.query(
        "SELECT * FROM " + TABLE_NAME + " WHERE path IN (:paths)",
        parameterSource,
        new FileStateRowMapper());
    for (FileState fileState : fileStates) {
      fileStateMap.put(fileState.getPath(), fileState);
    }
    return fileStateMap;
  }

  public List<FileState> getAll() {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.query("SELECT * FROM " + TABLE_NAME,
        new FileStateRowMapper());
  }

  public void deleteByPath(String path, boolean recursive) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String sql = "DELETE FROM " + TABLE_NAME + " WHERE path = ?";
    jdbcTemplate.update(sql, path);
    if (recursive) {
      sql = "DELETE FROM " + TABLE_NAME + " WHERE path LIKE ?";
      jdbcTemplate.update(sql, path + "/%");
    }
  }

  public int[] batchDelete(final List<String> paths) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    final String sql = "DELETE FROM " + TABLE_NAME + " WHERE path = ?";
    return jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
      @Override
      public void setValues(PreparedStatement ps, int i) throws SQLException {
        ps.setString(1, paths.get(i));
      }

      @Override
      public int getBatchSize() {
            return paths.size();
      }
    });
  }

  public void deleteAll() {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    final String sql = "DELETE FROM " + TABLE_NAME;
    jdbcTemplate.execute(sql);
  }

  class FileStateRowMapper implements RowMapper<FileState> {
    @Override
    public FileState mapRow(ResultSet resultSet, int i)
        throws SQLException {
      return new FileState(resultSet.getString("path"),
          FileState.FileType.fromValue(resultSet.getInt("type")),
          FileState.FileStage.fromValue(resultSet.getInt("stage")));
    }
  }
}
