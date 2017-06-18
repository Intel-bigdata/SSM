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
import org.apache.hadoop.fs.permission.FsPermission;
import org.smartdata.server.metastore.FileStatusInternal;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSourceUtils;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FileDao {
  private JdbcTemplate jdbcTemplate;
  private SimpleJdbcInsert simpleJdbcInsert;

  public FileDao(DataSource dataSource) {
    jdbcTemplate = new JdbcTemplate(dataSource);
    simpleJdbcInsert = new SimpleJdbcInsert(dataSource);
    simpleJdbcInsert.setTableName("files");
  }

  public List<FileStatusInternal> getAll() {
    return jdbcTemplate.query("SELECT * FROM files",
        new FileRowMapper());
  }

  public FileStatusInternal getById(long fid) {
    return jdbcTemplate.queryForObject("SELECT * FROM files WHERE fid = ?",
        new Object[]{fid}, new FileRowMapper());
  }

  public FileStatusInternal getByPath(String path) {
    return jdbcTemplate.queryForObject("SELECT * FROM files WHERE path = ?",
        new Object[]{path}, new FileRowMapper());
  }

  public Map<String, Long> getFids(Collection<String> paths)
      throws SQLException {
    Map<String, Long> pathToId = new HashMap<>();
    List<String> values = new ArrayList<>();
    for (String path : paths) {
      values.add("'" + path + "'");
    }
    String in = StringUtils.join(values, ", ");
    String sql = "SELECT * FROM files WHERE path IN (" + in + ")";
    List<FileStatusInternal> files = jdbcTemplate.query(sql,
        new FileRowMapper());
    for (FileStatusInternal file : files) {
      pathToId.put(file.getPath(), file.getFileId());
    }
    return pathToId;
  }

  public Map<Long, String> getPaths(Collection<Long> ids)
      throws SQLException {
    Map<Long, String> idToPath = new HashMap<>();
    List<String> values = new ArrayList<>();
    for (Long id : ids) {
      values.add("'" + id + "'");
    }
    String in = StringUtils.join(values, ", ");
    String sql = "SELECT * FROM files WHERE path IN (" + in + ")";
    List<FileStatusInternal> files = jdbcTemplate.query(sql,
        new FileRowMapper());
    for (FileStatusInternal file : files) {
      idToPath.put(file.getFileId(), file.getPath());
    }
    return idToPath;
  }

  public void insert(FileStatusInternal fileStatusInternal) {
    simpleJdbcInsert.execute(toMap(fileStatusInternal));
  }

  public void insert(FileStatusInternal[] fileStatusInternals) {
    // TODO need upgrade
    for (FileStatusInternal file : fileStatusInternals) {
      insert(file);
    }
  }

  public int update(String path, int policyId) {
    final String sql = "UPDATE files SET sid =? WHERE path = ?;";
    return this.jdbcTemplate.update(sql, policyId, path);
  }

  public void deleteById(long fid) {
    final String sql = "delete from files where fid = ?";
    jdbcTemplate.update(sql, fid);
  }

  public void deleteAll() {
    final String sql = "DELETE from files";
    jdbcTemplate.execute(sql);
  }


  private Map<String, Object> toMap(FileStatusInternal fileStatusInternal) {
    Map<String, Object> parameters = new HashMap<>();
    parameters.put("path", fileStatusInternal.getPath());
    parameters.put("fid", fileStatusInternal.getFileId());
    parameters.put("length", fileStatusInternal.getLen());
    parameters.put("block_replication", fileStatusInternal.getReplication());
    parameters.put("block_size", fileStatusInternal.getBlockSize());
    parameters.put("modification_time", fileStatusInternal.getModificationTime());
    parameters.put("access_time", fileStatusInternal.getAccessTime());
    parameters.put("is_dir", fileStatusInternal.isDir());
    parameters.put("sid", fileStatusInternal.getOwner());
    parameters.put("oid", fileStatusInternal.getGroup());
    parameters.put("permission", fileStatusInternal.getPermission());
    return parameters;
  }

  class FileRowMapper implements RowMapper<FileStatusInternal> {

    @Override
    public FileStatusInternal mapRow(ResultSet resultSet, int i) throws SQLException {
      FileStatusInternal status = new FileStatusInternal(resultSet.getLong("length"),
          resultSet.getBoolean("is_dir"),
          resultSet.getInt("block_replication"),
          resultSet.getLong("block_size"),
          resultSet.getLong("modification_time"),
          resultSet.getLong("access_time"),
          new FsPermission(resultSet.getShort("permission")),
          String.valueOf(resultSet.getShort("oid")),
          String.valueOf(resultSet.getShort("gid")),
          null, // Not tracked for now
          resultSet.getString("path").getBytes(),
          "",
          resultSet.getLong("fid"),
          0,    // Not tracked for now, set to 0
          null, // Not tracked for now, set to null
          resultSet.getByte("sid"));
      return status;
    }
  }

}
