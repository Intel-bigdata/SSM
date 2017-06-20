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

import org.smartdata.server.metastore.StorageCapacity;
import org.smartdata.server.metastore.StoragePolicy;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StorageDao {

  private JdbcTemplate jdbcTemplate;
  private SimpleJdbcInsert simpleJdbcInsertStorages;
  private SimpleJdbcInsert simpleJdbcInsertStorage_policy;

  public StorageDao(DataSource dataSource) {
    this.jdbcTemplate = new JdbcTemplate(dataSource);
    this.simpleJdbcInsertStorages = new SimpleJdbcInsert(dataSource);
    simpleJdbcInsertStorages.setTableName("storages");
    this.simpleJdbcInsertStorage_policy = new SimpleJdbcInsert(dataSource);
    simpleJdbcInsertStorage_policy.setTableName("storages");
  }

  private Map<String, StorageCapacity> getStorageTablesItem()
      throws SQLException {
    String sql = "SELECT * FROM storages";
    List<StorageCapacity> list = jdbcTemplate.queryForList(sql,StorageCapacity.class);
    Map<String,StorageCapacity> map = new HashMap<>();
    for (StorageCapacity s:list) {
      map.put(s.getType(),s);
    }
    return map;
  }

  public StorageCapacity getStorageCapacity(String type) throws SQLException {
    String sql = "SELECT * FROM storages WHERE type = ?";
    return jdbcTemplate.queryForObject(sql, new Object[]{type}, new RowMapper<StorageCapacity>() {
      public StorageCapacity mapRow(ResultSet rs, int rowNum) throws SQLException {
        StorageCapacity storageCapacity = new StorageCapacity(rs.getString("type"),
            rs.getLong("capacity"), rs.getLong("free"));
        return storageCapacity;
      }
    });
  }

  public String getStoragePolicyName(int sid) throws SQLException {
    String sql = "SELECT policy_name FROM `storage_policy` WHERE sid = ?";
    return jdbcTemplate.queryForObject(sql, new Object[]{sid}, String.class);
  }

  public Integer getStoragePolicyID(String policyName) throws SQLException {
    String sql = "SELECT sid FROM `storage_policy` WHERE policy_name = ?";
    return jdbcTemplate.queryForObject(sql, new Object[]{policyName}, Integer.class);
  }

  public synchronized void insertStoragePolicyTable(StoragePolicy s)
      throws SQLException {
    String sql = "INSERT INTO `storage_policy` (sid, policy_name) VALUES('"
        + s.getSid() + "','" + s.getPolicyName() + "');";
    jdbcTemplate.execute(sql);
  }

  public int updateFileStoragePolicy(String path, String policyName)
      throws SQLException {
    String sql0 = "SELECT sid FROM `storage_policy` WHERE policy_name = ?";
    Integer sid = jdbcTemplate.queryForObject(sql0, new Object[]{policyName}, Integer.class);
    if (sid == null) {
      throw new SQLException("Unknown storage policy name '"
          + policyName + "'");
    }
    String sql = String.format(
        "UPDATE files SET sid = %d WHERE path = '%s';",
        sid, path);
    return jdbcTemplate.update(sql);
  }

  public void insertStoragesTable(final StorageCapacity[] storages)
      throws SQLException {
    String sql = "INSERT INTO `storages` (type, capacity, free) VALUES (?,?,?);";
    jdbcTemplate.batchUpdate(sql,
        new BatchPreparedStatementSetter() {
          public void setValues(PreparedStatement ps, int i) throws SQLException {
            ps.setString(1, storages[i].getType());
            ps.setLong(2, storages[i].getCapacity());
            ps.setLong(3, storages[i].getFree());
          }

          public int getBatchSize() {
            return storages.length;
          }
        });
  }

  public synchronized boolean updateStoragesTable(String type
      , Long capacity, Long free) throws SQLException {
    String sql = null;
    String sqlPrefix = "UPDATE storages SET";
    String sqlCapacity = (capacity != null) ? ", capacity = '"
        + capacity + "'" : null;
    String sqlFree = (free != null) ? ", free = '" + free + "' " : null;
    String sqlSuffix = "WHERE type = '" + type + "';";
    if (capacity != null || free != null) {
      sql = sqlPrefix + sqlCapacity + sqlFree + sqlSuffix;
      sql = sql.replaceFirst(",", "");
    }
    return jdbcTemplate.update(sql) == 1;
  }
}
