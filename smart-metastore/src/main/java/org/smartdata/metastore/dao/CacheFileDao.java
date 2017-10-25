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

import org.apache.commons.collections.CollectionUtils;
import org.smartdata.metrics.FileAccessEvent;
import org.smartdata.model.CachedFileStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;

import javax.sql.DataSource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CacheFileDao {

  private DataSource dataSource;

  public void setDataSource(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public CacheFileDao(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public List<CachedFileStatus> getAll() {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.query("SELECT * FROM cached_file",
        new CacheFileRowMapper());
  }

  public CachedFileStatus getById(long fid) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.queryForObject("SELECT * FROM cached_file WHERE fid = ?",
        new Object[]{fid}, new CacheFileRowMapper());
  }

  public List<Long> getFids() {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String sql = "SELECT fid FROM cached_file";
    List<Long> fids = jdbcTemplate.query(sql,
        new RowMapper<Long>() {
          public Long mapRow(ResultSet rs, int rowNum) throws SQLException {
            return rs.getLong("fid");
          }
        });
    return fids;
  }

  public void insert(CachedFileStatus cachedFileStatus) {
    SimpleJdbcInsert simpleJdbcInsert = new SimpleJdbcInsert(dataSource);
    simpleJdbcInsert.setTableName("cached_file");
    simpleJdbcInsert.execute(toMap(cachedFileStatus));
  }

  public void insert(long fid, String path, long fromTime,
                     long lastAccessTime, int numAccessed) {
    SimpleJdbcInsert simpleJdbcInsert = new SimpleJdbcInsert(dataSource);
    simpleJdbcInsert.setTableName("cached_file");
    simpleJdbcInsert.execute(toMap(new CachedFileStatus(fid, path,
        fromTime, lastAccessTime, numAccessed)));
  }

  public void insert(CachedFileStatus[] cachedFileStatusList) {
    SimpleJdbcInsert simpleJdbcInsert = new SimpleJdbcInsert(dataSource);
    simpleJdbcInsert.setTableName("cached_file");

    Map<String, Object>[] maps = new Map[cachedFileStatusList.length];
    for (int i = 0; i < cachedFileStatusList.length; i++) {
      maps[i] = toMap(cachedFileStatusList[i]);
    }
    simpleJdbcInsert.executeBatch(maps);
  }

  public int update(Long fid, Long lastAccessTime, Integer numAccessed) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String sql = "UPDATE cached_file SET last_access_time = ?, accessed_num = ? WHERE fid = ?";
    return jdbcTemplate.update(sql, lastAccessTime, numAccessed, fid);
  }

  public void update(Map<String, Long> pathToIds,
                     List<FileAccessEvent> events) {
    Map<Long, CachedFileStatus> idToStatus = new HashMap<>();
    List<CachedFileStatus> cachedFileStatuses = getAll();
    for (CachedFileStatus status : cachedFileStatuses) {
      idToStatus.put(status.getFid(), status);
    }
    Collection<Long> cachedIds = idToStatus.keySet();
    Collection<Long> needToUpdate = CollectionUtils.intersection(cachedIds, pathToIds.values());
    if (!needToUpdate.isEmpty()) {
      Map<Long, Integer> idToCount = new HashMap<>();
      Map<Long, Long> idToLastTime = new HashMap<>();
      for (FileAccessEvent event : events) {
        Long fid = pathToIds.get(event.getPath());
        if (needToUpdate.contains(fid)) {
          if (!idToCount.containsKey(fid)) {
            idToCount.put(fid, 0);
          }
          idToCount.put(fid, idToCount.get(fid) + 1);
          if (!idToLastTime.containsKey(fid)) {
            idToLastTime.put(fid, event.getTimestamp());
          }
          idToLastTime.put(fid, Math.max(event.getTimestamp(), idToLastTime.get(fid)));
        }
      }
      for (Long fid : needToUpdate) {
        Integer newAccessCount = idToStatus.get(fid).getNumAccessed() + idToCount.get(fid);
        this.update(fid, idToLastTime.get(fid), newAccessCount);
      }
    }
  }

  public void deleteById(long fid) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    final String sql = "DELETE FROM cached_file WHERE fid = ?";
    jdbcTemplate.update(sql, fid);
  }

  public void deleteAll() {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String sql = "DELETE FROM cached_file";
    jdbcTemplate.execute(sql);
  }

  private Map<String, Object> toMap(CachedFileStatus cachedFileStatus) {
    Map<String, Object> parameters = new HashMap<>();
    parameters.put("fid", cachedFileStatus.getFid());
    parameters.put("path", cachedFileStatus.getPath());
    parameters.put("from_time", cachedFileStatus.getFromTime());
    parameters.put("last_access_time", cachedFileStatus.getLastAccessTime());
    parameters.put("accessed_num", cachedFileStatus.getNumAccessed());
    return parameters;
  }


  class CacheFileRowMapper implements RowMapper<CachedFileStatus> {

    @Override
    public CachedFileStatus mapRow(ResultSet resultSet, int i) throws SQLException {
      CachedFileStatus cachedFileStatus = new CachedFileStatus();
      cachedFileStatus.setFid(resultSet.getLong("fid"));
      cachedFileStatus.setPath(resultSet.getString("path"));
      cachedFileStatus.setFromTime(resultSet.getLong("from_time"));
      cachedFileStatus.setLastAccessTime(resultSet.getLong("last_access_time"));
      cachedFileStatus.setNumAccessed(resultSet.getInt("accessed_num"));
      return cachedFileStatus;
    }
  }
}
