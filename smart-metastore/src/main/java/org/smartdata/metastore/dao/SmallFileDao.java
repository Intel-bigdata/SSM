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
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import javax.sql.DataSource;

import java.sql.PreparedStatement;
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

  public void insertUpdate(CompactFileState compactFileState) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String sql = "REPLACE INTO small_file (path, container_file_path, offset, length)"
        + " VALUES (?,?,?,?)";
    jdbcTemplate.update(sql, compactFileState.getPath(),
        compactFileState.getFileContainerInfo().getContainerFilePath(),
        compactFileState.getFileContainerInfo().getOffset(),
        compactFileState.getFileContainerInfo().getLength());
  }

  public int[] batchInsertUpdate(final CompactFileState[] fileStates) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String sql = "REPLACE INTO small_file (path, container_file_path, offset, length)"
        + " VALUES (?,?,?,?)";
    return jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
      @Override
      public void setValues(PreparedStatement ps,
          int i) throws SQLException {
        ps.setString(1, fileStates[i].getPath());
        ps.setString(2, fileStates[i].getFileContainerInfo().getContainerFilePath());
        ps.setLong(3, fileStates[i].getFileContainerInfo().getOffset());
        ps.setLong(4, fileStates[i].getFileContainerInfo().getLength());
      }
      @Override
      public int getBatchSize() {
        return fileStates.length;
      }
    });
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

  public int[] batchDelete(final List<String> paths) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    final String sql = "DELETE FROM small_file WHERE path = ?";
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

  public FileState getFileStateByPath(String path) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.queryForObject("SELECT * FROM small_file WHERE path = ?",
        new Object[]{path}, new FileStateRowMapper());
  }

  public List<String> getSmallFilesByContainerFile(String containerFilePath) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String sql = "SELECT path FROM small_file where container_file_path = ?";
    return jdbcTemplate.queryForList(sql, String.class, containerFilePath);
  }

  public List<String> getAllContainerFiles() {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String sql = "SELECT DISTINCT container_file_path FROM small_file";
    return jdbcTemplate.queryForList(sql, String.class);
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
