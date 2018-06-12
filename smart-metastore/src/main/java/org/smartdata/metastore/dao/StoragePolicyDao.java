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

import org.smartdata.model.StoragePolicy;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StoragePolicyDao {
    private DataSource dataSource;

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public StoragePolicyDao(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    private static final String TABLE_NAME = "storage_policy";

    public Map<Integer, String> getStoragePolicyIdNameMap() throws SQLException {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        String sql = "SELECT * FROM " + TABLE_NAME;
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

    public List<StoragePolicy> getAll() {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        return jdbcTemplate.query("SELECT * FROM " + TABLE_NAME,
                new StoragePolicyRowMapper());
    }

    public String getStoragePolicyName(int sid) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        String sql = "SELECT policy_name FROM " + TABLE_NAME + " WHERE sid = ?";
        return jdbcTemplate.queryForObject(sql, new Object[]{sid}, String.class);
    }

    public synchronized void insertStoragePolicyTable(StoragePolicy s) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        String sql = "INSERT INTO storage_policy (sid, policy_name) VALUES('"
                + s.getSid() + "','" + s.getPolicyName() + "');";
        jdbcTemplate.execute(sql);
    }

    public void deleteStoragePolicy(int sid) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        final String sql = "DELETE FROM " + TABLE_NAME + " WHERE sid = ?";
        jdbcTemplate.update(sql, sid);
    }

    class StoragePolicyRowMapper implements RowMapper<StoragePolicy> {
        @Override
        public StoragePolicy mapRow(ResultSet resultSet, int i) throws SQLException {
            return new StoragePolicy(resultSet.getByte("sid"), resultSet.getString("policy_name"));
        }
    }
}
