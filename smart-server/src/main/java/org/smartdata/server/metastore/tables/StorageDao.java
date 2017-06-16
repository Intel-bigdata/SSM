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
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class StorageDao {

  private JdbcTemplate jdbcTemplate;
  private SimpleJdbcInsert simpleJdbcInsert;
  private Map<Integer, String> mapStoragePolicyIdName = null;
  private Map<String, Integer> mapStoragePolicyNameId = null;
  private Map<String, StorageCapacity> mapStorageCapacity = null;

  public StorageDao(DataSource dataSource) {
    this.jdbcTemplate = new JdbcTemplate(dataSource);
    this.simpleJdbcInsert = new SimpleJdbcInsert(dataSource);
    simpleJdbcInsert.setTableName("storages");
  }

  private Map<String, StorageCapacity> convertStorageTablesItem(
      ResultSet resultSet) throws SQLException {
    Map<String, StorageCapacity> map = new HashMap<>();
    if (resultSet == null) {
      return map;
    }

    while (resultSet.next()) {
      String type = resultSet.getString(1);
      StorageCapacity storage = new StorageCapacity(
          resultSet.getString(1),
          resultSet.getLong(2),
          resultSet.getLong(3));
      map.put(type, storage);
    }
    return map;
  }

  public StorageCapacity getStorageCapacity(String type) throws SQLException {
//    updateCache();
    return mapStorageCapacity.get(type);
  }

  public String getStoragePolicyName(int sid) throws SQLException {
//    updateCache();
    return mapStoragePolicyIdName.get(sid);
  }

  public Integer getStoragePolicyID(String policyName) throws SQLException {
//    updateCache();
    return getKey(mapStoragePolicyIdName, policyName);
  }

  public synchronized void insertStoragePolicyTable(StoragePolicy s)
      throws SQLException {
    String sql = "INSERT INTO `storage_policy` (sid, policy_name) VALUES('"
        + s.getSid() + "','" + s.getPolicyName() + "');";
    mapStoragePolicyIdName = null;
    jdbcTemplate.execute(sql);
  }

  public int updateFileStoragePolicy(String path, String policyName)
      throws SQLException {
    if (mapStoragePolicyIdName == null) {
      //updateCache();
    }
    if (!mapStoragePolicyNameId.containsKey(policyName)) {
      throw new SQLException("Unknown storage policy name '"
          + policyName + "'");
    }
    String sql = String.format(
        "UPDATE files SET sid = %d WHERE path = '%s';",
        mapStoragePolicyNameId.get(policyName), path);
    return jdbcTemplate.update(sql);
  }

  public void insertStoragesTable(final StorageCapacity[] storages)
      throws SQLException {
    mapStorageCapacity = null;
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
    mapStorageCapacity = null;
    return jdbcTemplate.update(sql) == 1;
  }

  private Integer getKey(Map<Integer, String> map, String value) {
    for (Integer key : map.keySet()) {
      if (map.get(key).equals(value)) {
        return key;
      }
    }
    return null;
  }

}
