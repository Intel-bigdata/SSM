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

import org.smartdata.metastore.MetaStoreException;
import org.smartdata.model.ErasureCodingPolicyInfo;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;

import javax.sql.DataSource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ErasureCodingPolicyDao {
  private static final String TABLE_NAME = "ec_policy";
  private static final String ID = "id";
  private static final String NAME = "policy_name";
  private DataSource dataSource;

  public ErasureCodingPolicyDao(DataSource dataSource) throws MetaStoreException {
    this.dataSource = dataSource;
  }

  public ErasureCodingPolicyInfo getEcPolicyById(byte id) throws MetaStoreException {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.queryForObject
        ("SELECT * FROM " + TABLE_NAME + " WHERE id=?", new Object[]{id}, new EcPolicyRowMapper());
  }

  public ErasureCodingPolicyInfo getEcPolicyByName(String policyName) throws MetaStoreException {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.queryForObject("SELECT * FROM " + TABLE_NAME + " WHERE policy_name=?",
        new Object[]{policyName}, new EcPolicyRowMapper());
  }

  public List<ErasureCodingPolicyInfo> getAllEcPolicies() {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    return jdbcTemplate.query("SELECT * FROM " + TABLE_NAME, new EcPolicyRowMapper());
  }

  public void insert(ErasureCodingPolicyInfo ecPolicy) {
    SimpleJdbcInsert simpleJdbcInsert = new SimpleJdbcInsert(dataSource);
    simpleJdbcInsert.setTableName(TABLE_NAME);
    simpleJdbcInsert.execute(toMap(ecPolicy));
  }

  public void insert(List<ErasureCodingPolicyInfo> ecInfos) {
    SimpleJdbcInsert simpleJdbcInsert = new SimpleJdbcInsert(dataSource);
    simpleJdbcInsert.setTableName(TABLE_NAME);
    Map<String, Object>[] maps = new Map[ecInfos.size()];
    for (int i = 0; i < ecInfos.size(); i++) {
      maps[i] = toMap(ecInfos.get(i));
    }
    simpleJdbcInsert.executeBatch(maps);
  }


  public void deleteAll() {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    final String sql = "DELETE FROM " + TABLE_NAME;
    jdbcTemplate.execute(sql);
  }

  private Map<String, Object> toMap(ErasureCodingPolicyInfo ecPolicy) {
    Map<String, Object> map = new HashMap<>();
    map.put(ID, ecPolicy.getID());
    map.put(NAME, ecPolicy.getEcPolicyName());
    return map;
  }

  class EcPolicyRowMapper implements RowMapper<ErasureCodingPolicyInfo> {
    @Override
    public ErasureCodingPolicyInfo mapRow(ResultSet resultSet, int i) throws SQLException {
      ErasureCodingPolicyInfo ecPolicy = new ErasureCodingPolicyInfo(resultSet.getByte("id"),
          resultSet.getString("policy_name"));
      return ecPolicy;
    }
  }
}
