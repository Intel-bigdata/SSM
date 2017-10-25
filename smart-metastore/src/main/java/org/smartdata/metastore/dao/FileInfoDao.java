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

import org.smartdata.metastore.utils.MetaStoreUtils;
import org.smartdata.model.FileInfo;
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

public class FileInfoDao {

  private DataSource dataSource;
  private Map<Integer, String> mapOwnerIdName;
  private Map<Integer, String> mapGroupIdName;

  public void setDataSource(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public FileInfoDao(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public void updateUsersMap(Map<Integer, String> mapOwnerIdName) {
    this.mapOwnerIdName = mapOwnerIdName;
  }

  public void updateGroupsMap(Map<Integer, String> mapGroupIdName) {
    this.mapGroupIdName = mapGroupIdName;
  }

  public List<FileInfo> getAll() {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.query("SELECT * FROM file",
        new FileInfoDao.FileInfoRowMapper());
  }

  public List<FileInfo> getFilesByPrefix(String path) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.query("SELECT * FROM file WHERE path LIKE ?",
        new FileInfoDao.FileInfoRowMapper(), path + "%");
  }

  public FileInfo getById(long fid) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.queryForObject("SELECT * FROM file WHERE fid = ?",
        new Object[]{fid}, new FileInfoDao.FileInfoRowMapper());
  }

  public FileInfo getByPath(String path) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.queryForObject("SELECT * FROM file WHERE path = ?",
        new Object[]{path}, new FileInfoDao.FileInfoRowMapper());
  }

  public Map<String, Long> getPathFids(Collection<String> paths)
      throws SQLException {
    NamedParameterJdbcTemplate namedParameterJdbcTemplate =
        new NamedParameterJdbcTemplate(dataSource);
    Map<String, Long> pathToId = new HashMap<>();
    String sql = "SELECT * FROM file WHERE path IN (:paths)";
    MapSqlParameterSource parameterSource = new MapSqlParameterSource();
    parameterSource.addValue("paths", paths);
    List<FileInfo> files = namedParameterJdbcTemplate.query(sql,
        parameterSource, new FileInfoRowMapper());
    for (FileInfo file : files) {
      pathToId.put(file.getPath(), file.getFileId());
    }
    return pathToId;
  }

  public Map<Long, String> getFidPaths(Collection<Long> ids)
      throws SQLException {
    NamedParameterJdbcTemplate namedParameterJdbcTemplate =
        new NamedParameterJdbcTemplate(dataSource);
    Map<Long, String> idToPath = new HashMap<>();
    String sql = "SELECT * FROM file WHERE fid IN (:ids)";
    MapSqlParameterSource parameterSource = new MapSqlParameterSource();
    parameterSource.addValue("ids", ids);
    List<FileInfo> files = namedParameterJdbcTemplate.query(sql,
        parameterSource, new FileInfoRowMapper());
    for (FileInfo file : files) {
      idToPath.put(file.getFileId(), file.getPath());
    }
    return idToPath;
  }

  public void insert(FileInfo fileInfo) {
    SimpleJdbcInsert simpleJdbcInsert = new SimpleJdbcInsert(dataSource);
    simpleJdbcInsert.setTableName("file");
    simpleJdbcInsert.execute(toMap(fileInfo,
        mapOwnerIdName, mapGroupIdName));
  }

  public void insert(FileInfo[] fileInfos) {
    SimpleJdbcInsert simpleJdbcInsert = new SimpleJdbcInsert(dataSource);
    simpleJdbcInsert.setTableName("file");
    Map<String, Object>[] maps = new Map[fileInfos.length];
    for (int i = 0; i < fileInfos.length; i++) {
      maps[i] = toMap(fileInfos[i], mapOwnerIdName, mapGroupIdName);
    }
    simpleJdbcInsert.executeBatch(maps);
  }

  public int update(String path, int storagePolicy) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    final String sql = "UPDATE file SET sid =? WHERE path = ?;";
    return jdbcTemplate.update(sql, storagePolicy, path);
  }

  public void deleteById(long fid) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    final String sql = "DELETE FROM file WHERE fid = ?";
    jdbcTemplate.update(sql, fid);
  }

  public void deleteAll() {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    final String sql = "DELETE FROM file";
    jdbcTemplate.execute(sql);
  }

  private Map<String, Object> toMap(FileInfo fileInfo
      , Map<Integer, String> mapOwnerIdName
      , Map<Integer, String> mapGroupIdName) {
    Map<String, Object> parameters = new HashMap<>();
    parameters.put("path", fileInfo.getPath());
    parameters.put("fid", fileInfo.getFileId());
    parameters.put("length", fileInfo.getLength());
    parameters.put("block_replication", fileInfo.getBlockReplication());
    parameters.put("block_size", fileInfo.getBlocksize());
    parameters.put("modification_time", fileInfo.getModificationTime());
    parameters.put("access_time", fileInfo.getAccessTime());
    parameters.put("is_dir", fileInfo.isdir());
    parameters.put("sid", fileInfo.getStoragePolicy());
    parameters
        .put("oid", MetaStoreUtils.getKey(mapOwnerIdName, fileInfo.getOwner()));
    parameters
        .put("gid", MetaStoreUtils.getKey(mapGroupIdName, fileInfo.getGroup()));
    parameters.put("permission", fileInfo.getPermission());
    return parameters;
  }

  class FileInfoRowMapper implements RowMapper<FileInfo> {
    @Override
    public FileInfo mapRow(ResultSet resultSet, int i)
        throws SQLException {
      FileInfo fileInfo = new FileInfo(resultSet.getString("path"),
          resultSet.getLong("fid"),
          resultSet.getLong("length"),
          resultSet.getBoolean("is_dir"),
          resultSet.getShort("block_replication"),
          resultSet.getLong("block_size"),
          resultSet.getLong("modification_time"),
          resultSet.getLong("access_time"),
          resultSet.getShort("permission"),
          mapOwnerIdName.get((int) resultSet.getShort("oid")),
          mapGroupIdName.get((int) resultSet.getShort("gid")),
          resultSet.getByte("sid")
      );
      return fileInfo;
    }
  }
}
