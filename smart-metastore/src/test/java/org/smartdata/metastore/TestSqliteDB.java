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
package org.smartdata.metastore;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.smartdata.metastore.utils.MetaStoreUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/** Test operations with sqlite database. */
public class TestSqliteDB extends TestDaoUtil {

  private MetaStore metaStore;

  @Before
  public void initDB() throws Exception {
    initDao();
    metaStore = new MetaStore(druidPool);
  }

  @After
  public void closeDB() throws Exception {
    metaStore = null;
    closeDao();
  }

  @Test
  public void testInitDB() throws Exception {
    MetaStoreUtils.initializeDataBase(metaStore.getConnection());
  }

  @Test
  public void testDropTables() throws Exception {
    Connection conn = metaStore.getConnection();
    Statement s = conn.createStatement();
    metaStore.dropAllTables();
    for (int i = 0; i < 10; i++) {
      metaStore.execute("DROP TABLE IF EXISTS tb_" + i + ";");
      metaStore.execute("CREATE TABLE tb_" + i + " (a INT(11));");
    }
    String dbUrl = conn.getMetaData().getURL();
    if (dbUrl.startsWith(MetaStoreUtils.SQLITE_URL_PREFIX)) {
      ResultSet rs = s.executeQuery("select tbl_name from sqlite_master;");
      List<String> list = new ArrayList<>();
      while (rs.next()) {
        list.add(rs.getString(1));
      }
      metaStore.dropAllTables();
      rs = s.executeQuery("select tbl_name from sqlite_master;");
      List<String> list1 = new ArrayList<>();
      while (rs.next()) {
        list1.add(rs.getString(1));
      }
      Assert.assertEquals(10, list.size() - list1.size());
    } else {
      String dbName;
      if (dbUrl.contains("?")) {
        dbName = dbUrl.substring(dbUrl.indexOf("/", 13) + 1, dbUrl.indexOf("?"));
      } else {
        dbName = dbUrl.substring(dbUrl.lastIndexOf("/") + 1, dbUrl.length());
      }
      ResultSet rs =
          s.executeQuery(
              "SELECT TABLE_NAME FROM "
                  + "INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '"
                  + dbName
                  + "';");
      List<String> list = new ArrayList<>();
      while (rs.next()) {
        list.add(rs.getString(1));
      }
      metaStore.dropAllTables();
      rs =
          s.executeQuery(
              "SELECT TABLE_NAME FROM "
                  + "INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '"
                  + dbName
                  + "';");
      List<String> list1 = new ArrayList<>();
      while (rs.next()) {
        list1.add(rs.getString(1));
      }
      Assert.assertEquals(10, list.size() - list1.size());
    }
    conn.close();
  }

  @Test
  public void testDBBlankStatements() throws Exception {
    String[] presqls =
        new String[] {
          "INSERT INTO rule (state, rule_text, submit_time, checked_count, "
              + "generated_cmdlets) VALUES (0, 'file: every 1s \n"
              + " | "
              + "accessCount(5s) > 3 | cache', 1494903787619, 0, 0);"
        };

    for (int i = 0; i < presqls.length; i++) {
      String sql = presqls[i];
      metaStore.execute(sql);
    }

    String[] sqls =
        new String[] {
          "DROP TABLE IF EXISTS VIR_ACC_CNT_TAB_1_accessCount_5000;",
          "CREATE TABLE VIR_ACC_CNT_TAB_1_accessCount_5000 "
              + "AS SELECT * FROM blank_access_count_info;",
          "SELECT fid from VIR_ACC_CNT_TAB_1_accessCount_5000;",
          "SELECT path FROM file WHERE (fid IN (SELECT fid FROM "
              + "VIR_ACC_CNT_TAB_1_accessCount_5000 WHERE ((count > 3))));"
        };

    for (int i = 0; i < sqls.length * 3; i++) {
      int idx = i % sqls.length;
      String sql = sqls[idx];
      metaStore.execute(sql);
    }
  }
}
