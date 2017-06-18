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

import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class XattrDao {
  private JdbcTemplate jdbcTemplate;
  private SimpleJdbcInsert simpleJdbcInsert;

  public XattrDao(DataSource dataSource) {
    this.jdbcTemplate = new JdbcTemplate(dataSource);
    this.simpleJdbcInsert = new SimpleJdbcInsert(dataSource);
    simpleJdbcInsert.setTableName("xattr");
  }

  public Map<String, byte[]> getXattrTable(Long fid) throws SQLException {
    String sql =
        String.format("SELECT * FROM xattr WHERE fid = %s;", fid);
    return getXattrTable(sql);
  }

  public Map<String, byte[]> getXattrTable(String sql) throws SQLException {
    List<XAttr> list = new LinkedList<>();
    List<Map<String, Object>> maplist = jdbcTemplate.queryForList(sql);
    int i = 1;
    for (Map<String, Object> map : maplist) {
      XAttr xAttr = new XAttr.Builder()
          .setNameSpace(XAttr.NameSpace.valueOf((String) map.get("namespace")))
          .setName((String) map.get("name"))
          .setValue((byte[]) map.get("value")).build();
      list.add(xAttr);
    }
    return XAttrHelper.buildXAttrMap(list);
  }

  public synchronized boolean insertXattrTable(final Long fid, final Map<String,
      byte[]> map) throws SQLException {
    String sql = "INSERT INTO xattr (fid, namespace, name, value) "
        + "VALUES (?, ?, ?, ?)";
    final List<XAttr> xattrlist = new ArrayList<>();
    for (Map.Entry<String, byte[]> e : map.entrySet()) {
      XAttr xa = XAttrHelper.buildXAttr(e.getKey(), e.getValue());
      xattrlist.add(xa);
    }
    int[] i = jdbcTemplate.batchUpdate(sql,
        new BatchPreparedStatementSetter() {
          public void setValues(PreparedStatement ps, int i) throws SQLException {
            ps.setLong(1, fid);
            ps.setString(2, String.valueOf(xattrlist.get(i).getNameSpace()));
            ps.setString(3, xattrlist.get(i).getName());
            ps.setBytes(4, xattrlist.get(i).getValue());
          }

          public int getBatchSize() {
            return map.entrySet().size();
          }
        });
    if (i.length == map.size()) {
      return true;
    } else {
      return false;
    }
  }

}
