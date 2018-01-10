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

import org.smartdata.model.StorageCapacity;
import org.smartdata.model.StoragePolicy;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import javax.sql.DataSource;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StorageDao {
  private DataSource dataSource;

  public void setDataSource(DataSource dataSource) {
    this.dataSource = dataSource;
  }


  public StorageDao(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public Map<String, StorageCapacity> getStorageTablesItem() {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String sql = "SELECT * FROM storage";
    List<StorageCapacity> list = jdbcTemplate.query(sql,
        new RowMapper<StorageCapacity>() {
          public StorageCapacity mapRow(ResultSet rs,
              int rowNum) throws SQLException {
            return new StorageCapacity(rs.getString("type"), rs.getLong("time_stamp"),
                rs.getLong("capacity"), rs.getLong("free"));
          }
        });
    Map<String, StorageCapacity> map = new HashMap<>();
    for (StorageCapacity s : list) {
      map.put(s.getType(), s);
    }
    return map;
  }

  public Map<Integer, String> getStoragePolicyIdNameMap() throws SQLException {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String sql = "SELECT * FROM storage_policy";
    List<StoragePolicy> list = jdbcTemplate.query(sql,
        new RowMapper<StoragePolicy>() {
          public StoragePolicy mapRow(ResultSet rs,
              int rowNum) throws SQLException {
            return new StoragePolicy(rs.getByte("sid"),
                rs.getString("policy_name"));
          }
        });
    Map<Integer, String> map = new HashMap<>();
    for (StoragePolicy s : list) {
      map.put((int) (s.getSid()), s.getPolicyName());
    }
    return map;
  }

  public StorageCapacity getStorageCapacity(String type) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String sql = "SELECT * FROM storage WHERE type = ?";
    return jdbcTemplate.queryForObject(sql, new Object[]{type},
        new RowMapper<StorageCapacity>() {
          public StorageCapacity mapRow(ResultSet rs,
              int rowNum) throws SQLException {
            return new StorageCapacity(rs.getString("type"), rs.getLong("time_stamp"),
                rs.getLong("capacity"), rs.getLong("free"));
          }
        });
  }

  public String getStoragePolicyName(int sid) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String sql = "SELECT policy_name FROM storage_policy WHERE sid = ?";
    return jdbcTemplate.queryForObject(sql, new Object[]{sid}, String.class);
  }

  public Integer getStoragePolicyID(String policyName) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String sql = "SELECT sid FROM storage_policy WHERE policy_name = ?";
    return jdbcTemplate
        .queryForObject(sql, new Object[]{policyName}, Integer.class);
  }

  public synchronized void insertStoragePolicyTable(StoragePolicy s) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String sql = "INSERT INTO storage_policy (sid, policy_name) VALUES('"
        + s.getSid() + "','" + s.getPolicyName() + "');";
    jdbcTemplate.execute(sql);
  }

  public int updateFileStoragePolicy(String path,
      String policyName) throws SQLException {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String sql0 = "SELECT sid FROM storage_policy WHERE policy_name = ?";
    Integer sid = jdbcTemplate
        .queryForObject(sql0, new Object[]{policyName}, Integer.class);
    if (sid == null) {
      throw new SQLException("Unknown storage policy name '"
          + policyName + "'");
    }
    String sql = String.format(
        "UPDATE file SET sid = %d WHERE path = '%s';",
        sid, path);
    return jdbcTemplate.update(sql);
  }

  public void insertUpdateStoragesTable(final StorageCapacity[] storages)
      throws SQLException {
    if (storages.length == 0) {
      return;
    }
    final Long curr = System.currentTimeMillis();
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String sql = "REPLACE INTO storage (type, time_stamp, capacity, free) VALUES (?,?,?,?);";
    jdbcTemplate.batchUpdate(sql,
        new BatchPreparedStatementSetter() {
          public void setValues(PreparedStatement ps,
              int i) throws SQLException {
            ps.setString(1, storages[i].getType());
            if (storages[i].getTimeStamp() == null) {
              ps.setLong(2, curr);
            } else {
              ps.setLong(2, storages[i].getTimeStamp());
            }
            ps.setLong(3, storages[i].getCapacity());
            ps.setLong(4, storages[i].getFree());
          }

          public int getBatchSize() {
            return storages.length;
          }
        });
  }

  public int getCountOfStorageType(String type) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String sql = "SELECT COUNT(*) FROM storage WHERE type = ?";
    return jdbcTemplate.queryForObject(sql, Integer.class, type);
  }

  public void deleteStorage(String storageType) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    final String sql = "DELETE FROM storage WHERE type = ?";
    jdbcTemplate.update(sql, storageType);
  }

  public synchronized boolean updateStoragesTable(String type, Long timeStamp,
      Long capacity, Long free) throws SQLException {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    String sql = null;
    String sqlPrefix = "UPDATE storage SET";
    String sqlCapacity = (capacity != null) ? ", capacity = '"
        + capacity + "' " : null;
    String sqlFree = (free != null) ? ", free = '" + free + "' " : null;
    String sqlTimeStamp = (timeStamp != null) ? ", time_stamp = " + timeStamp + " " : null;
    String sqlSuffix = "WHERE type = '" + type + "';";
    if (capacity != null || free != null) {
      sql = sqlPrefix + sqlCapacity + sqlFree + sqlTimeStamp + sqlSuffix;
      sql = sql.replaceFirst(",", "");
    }
    return jdbcTemplate.update(sql) == 1;
  }

  public synchronized boolean updateStoragesTable(String type,
      Long capacity, Long free) throws SQLException {
    return updateStoragesTable(type, System.currentTimeMillis(), capacity, free);
  }
}
