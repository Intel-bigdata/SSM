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

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.smartdata.model.FileStatusInternal;
import org.smartdata.metastore.utils.MetaStoreUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;

import javax.sql.DataSource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FileDao {
  private DataSource dataSource;
  private Map<Integer, String> mapOwnerIdName;
  private Map<Integer, String> mapGroupIdName;

  public void setDataSource(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public FileDao(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public void updateUsersMap(Map<Integer, String> mapOwnerIdName) {
    this.mapOwnerIdName = mapOwnerIdName;
  }

  public void updateGroupsMap(Map<Integer, String> mapGroupIdName) {
    this.mapGroupIdName = mapGroupIdName;
  }

  public List<HdfsFileStatus> getAll() {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.query("SELECT * FROM files",
        new FileRowMapper());
  }

  public HdfsFileStatus getById(long fid) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.queryForObject("SELECT * FROM files WHERE fid = ?",
        new Object[]{fid}, new FileRowMapper());
  }

  public HdfsFileStatus getByPath(String path) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.queryForObject("SELECT * FROM files WHERE path = ?",
        new Object[]{path}, new FileRowMapper());
  }

  public Map<String, Long> getPathFids(Collection<String> paths) {
    NamedParameterJdbcTemplate namedParameterJdbcTemplate =
        new NamedParameterJdbcTemplate(dataSource);
    Map<String, Long> pathToId = new HashMap<>();
    String sql = "SELECT * FROM files WHERE path IN (:paths)";
    MapSqlParameterSource parameterSource = new MapSqlParameterSource();
    parameterSource.addValue("paths", paths);
    List<HdfsFileStatus> files = namedParameterJdbcTemplate.query(sql,
        parameterSource, new FileRowMapper());
    for (HdfsFileStatus file : files) {
      pathToId.put(file.getLocalName(), file.getFileId());
    }
    return pathToId;
  }

  public Map<Long, String> getFidPaths(Collection<Long> ids) {
    NamedParameterJdbcTemplate namedParameterJdbcTemplate =
        new NamedParameterJdbcTemplate(dataSource);
    Map<Long, String> idToPath = new HashMap<>();
    String sql = "SELECT * FROM files WHERE fid IN (:ids)";
    MapSqlParameterSource parameterSource = new MapSqlParameterSource();
    parameterSource.addValue("ids", ids);
    List<HdfsFileStatus> files = namedParameterJdbcTemplate.query(sql,
        parameterSource, new FileRowMapper());
    for (HdfsFileStatus file : files) {
      idToPath.put(file.getFileId(), file.getLocalName());
    }
    return idToPath;
  }

  public int getCount() {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.queryForObject("select count(*) from files", Integer.class);
  }

  public void insert(FileStatusInternal fileStatusInternal) {
    SimpleJdbcInsert simpleJdbcInsert = new SimpleJdbcInsert(dataSource);
    simpleJdbcInsert.setTableName("files");
    simpleJdbcInsert.execute(toMap(fileStatusInternal,
        mapOwnerIdName, mapGroupIdName));
  }

  public void insert(FileStatusInternal[] fileStatusInternals) {
    SimpleJdbcInsert simpleJdbcInsert = new SimpleJdbcInsert(dataSource);
    simpleJdbcInsert.setTableName("files");
    Map<String, Object>[] maps = new Map[fileStatusInternals.length];
    for (int i = 0; i < fileStatusInternals.length; i++) {
      maps[i] = toMap(fileStatusInternals[i],mapOwnerIdName,mapGroupIdName);
    }
    simpleJdbcInsert.executeBatch(maps);
  }

  public int update(String path, int policyId) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    final String sql = "UPDATE files SET sid =? WHERE path = ?;";
    return jdbcTemplate.update(sql, policyId, path);
  }

  public void deleteById(long fid) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    final String sql = "delete from files where fid = ?";
    jdbcTemplate.update(sql, fid);
  }

  public void deleteAll() {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    final String sql = "DELETE from files";
    jdbcTemplate.execute(sql);
  }

  private Map<String, Object> toMap(FileStatusInternal fileStatusInternal,
      Map<Integer, String> mapOwnerIdName, Map<Integer, String> mapGroupIdName) {
    Map<String, Object> parameters = new HashMap<>();
    parameters.put("path", fileStatusInternal.getPath());
    parameters.put("fid", fileStatusInternal.getFileId());
    parameters.put("length", fileStatusInternal.getLen());
    parameters.put("block_replication", fileStatusInternal.getReplication());
    parameters.put("block_size", fileStatusInternal.getBlockSize());
    parameters.put("modification_time", fileStatusInternal.getModificationTime());
    parameters.put("access_time", fileStatusInternal.getAccessTime());
    parameters.put("is_dir", fileStatusInternal.isDir());
    parameters.put("sid", fileStatusInternal.getStoragePolicy());
    parameters.put("oid", MetaStoreUtils.getKey(mapOwnerIdName, fileStatusInternal.getOwner()));
    parameters.put("gid", MetaStoreUtils.getKey(mapGroupIdName, fileStatusInternal.getGroup()));
    parameters.put("permission", fileStatusInternal.getPermission().toShort());
    return parameters;
  }


  class FileRowMapper implements RowMapper<HdfsFileStatus> {

    @Override
    public HdfsFileStatus mapRow(ResultSet resultSet,
        int i) throws SQLException {
      FileStatusInternal status = new FileStatusInternal(resultSet.getLong("length"),
          resultSet.getBoolean("is_dir"),
          resultSet.getInt("block_replication"),
          resultSet.getLong("block_size"),
          resultSet.getLong("modification_time"),
          resultSet.getLong("access_time"),
          new FsPermission(resultSet.getShort("permission")),
          mapOwnerIdName.get((int)resultSet.getShort("oid")),
          mapGroupIdName.get((int)resultSet.getShort("gid")),
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
