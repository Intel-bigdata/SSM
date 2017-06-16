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

import org.smartdata.common.metastore.CachedFileStatus;
import org.smartdata.metrics.FileAccessEvent;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;


public class CacheFileDao {
  private JdbcTemplate jdbcTemplate;
  private SimpleJdbcInsert simpleJdbcInsert;

  public CacheFileDao(DataSource dataSource) {
    jdbcTemplate = new JdbcTemplate(dataSource);
    simpleJdbcInsert = new SimpleJdbcInsert(dataSource);
    simpleJdbcInsert.setTableName("cached_files");
  }

  public List<CachedFileStatus> getAllCachedFileStatus() {
    return jdbcTemplate.query("select * from cached_files",
        new CacheFileRowMapper());
  }

  public CachedFileStatus getCachedFileStatusById(long fid) {
    return jdbcTemplate.queryForObject("select * from actions where fid = ?",
        new Object[]{fid}, new CacheFileRowMapper());
  }

  public List<Long> getCachedFids() {

  }

  public void insertCacheFile(CachedFileStatus) {

  }

  public void insertCacheFiles(List<CachedFileStatus> cachedFileStatusList) {

  }

  public void updateCacheFiles(Map<String, Long> pathToIds,
      List<FileAccessEvent> events) {

  }

  public void deleteCachedFile(long fid) {
    final String sql = "delete from cached_files where fid = ?";
    jdbcTemplate.update(sql, fid);
  }

  public void deleteAll() {
    String sql = "DELETE from `cached_files`";
    jdbcTemplate.execute(sql);
  }

  class CacheFileRowMapper implements RowMapper<CachedFileStatus> {

    @Override
    public CachedFileStatus mapRow(ResultSet resultSet, int i) throws SQLException {
      CachedFileStatus cachedFileStatus = new CachedFileStatus();
      cachedFileStatus.setFid(resultSet.getLong("fid"));
      cachedFileStatus.setPath(resultSet.getString("path"));
      cachedFileStatus.setFromTime(resultSet.getLong("from_time"));
      cachedFileStatus.setLastAccessTime(resultSet.getLong("last_access_time"));
      cachedFileStatus.setNumAccessed(resultSet.getInt("num_accessed"));
      return cachedFileStatus;
    }
  }
}