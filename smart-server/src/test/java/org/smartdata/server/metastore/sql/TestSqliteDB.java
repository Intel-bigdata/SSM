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
package org.smartdata.server.metastore.sql;

import org.junit.Test;
import org.smartdata.server.metastore.DBAdapter;
import org.smartdata.server.metastore.TestDBUtil;
import org.smartdata.server.metastore.Util;

import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Test operations with sqlite database.
 */
public class TestSqliteDB {

  @Test
  public void testCreateNewSqliteDB() throws Exception {
    String dbFile = TestDBUtil.getUniqueDBFilePath();
    Connection conn = null;
    try {
      conn = Util.createSqliteConnection(dbFile);
      Util.initializeDataBase(conn);
    } finally {
      if (conn != null) {
        conn.close();
      }
      File file = new File(dbFile);
      file.deleteOnExit();
    }
  }

  @Test
  public void testSqliteDBBlankStatements() throws Exception {
    String dbFile = TestDBUtil.getUniqueDBFilePath();
    Connection conn = null;
    try {
      conn = Util.createSqliteConnection(dbFile);
      Util.initializeDataBase(conn);
      DBAdapter adapter = new DBAdapter(conn);

      String[] presqls = new String[] {
          "INSERT INTO rules (state, rule_text, submit_time, checked_count, "
              + "commands_generated) VALUES (0, 'file: every 1s \n" + " | "
              + "accessCount(5s) > 3 | cachefile', 1494903787619, 0, 0);"
      };

      for (int i = 0; i< presqls.length; i++) {
        String sql = presqls[i];
        adapter.execute(sql);
      }

      String[] sqls = new String[] {
          "DROP TABLE IF EXISTS 'VIR_ACC_CNT_TAB_1_accessCount_5000';",
          "CREATE TABLE 'VIR_ACC_CNT_TAB_1_accessCount_5000' "
              + "AS SELECT * FROM 'blank_access_count_info';",
          "SELECT fid from 'VIR_ACC_CNT_TAB_1_accessCount_5000';",
          "SELECT path FROM files WHERE (fid IN (SELECT fid FROM "
              + "'VIR_ACC_CNT_TAB_1_accessCount_5000' WHERE ((count > 3))));"
      };

      for (int i = 0; i< sqls.length * 3; i++) {
        int idx = i % sqls.length;
        String sql = sqls[idx];
        adapter.execute(sql);
      }
    } finally {
      if (conn != null) {
        conn.close();
      }
      File file = new File(dbFile);
      file.deleteOnExit();
    }
  }

  @Test
  public void testDropTablesSqlite() throws SQLException, ClassNotFoundException {
    String dbFile = TestDBUtil.getUniqueDBFilePath();
    Connection conn = null;
    try {
      conn = Util.createSqliteConnection(dbFile);
      Util.initializeDataBase(conn);
      DBAdapter adapter = new DBAdapter(conn);
      Statement s = conn.createStatement();
      adapter.dropAllTables();
      for (int i = 0; i < 10; i++) {
        adapter.execute("DROP TABLE IF EXISTS tb_"+i+";");
        adapter.execute("CREATE TABLE tb_"+i+" (a INT(11));");
      }
      ResultSet rs = s.executeQuery("select tbl_name from sqlite_master;");
      List<String> list = new ArrayList<>();
      while (rs.next()) {
        list.add(rs.getString(1));
      }
      adapter.dropAllTables();
      rs = s.executeQuery("select tbl_name from sqlite_master;");
      List<String> list1 = new ArrayList<>();
      while (rs.next()) {
        list1.add(rs.getString(1));
      }
      assertEquals(10,list.size()-list1.size());
    } finally {
      if (conn != null) {
        conn.close();
      }
      File file = new File(dbFile);
      file.deleteOnExit();
    }
  }

  /*@Test
  public void testDropAllTablesMysql() throws SQLException {
    Connection conn = null;
    try {
      String url = "jdbc:mysql://localhost:3306/";
      conn = DriverManager.getConnection(url, "root", "linux123");
      Statement s = conn.createStatement();
      String db = "abcd";
      s.executeUpdate("DROP DATABASE IF EXISTS "+db+";");
      s.executeUpdate("CREATE DATABASE "+db+";");
      s.execute("use "+db+";");
      conn = Util.createConnection(url+db+"?","root","linux123");
      DBAdapter adapter = new DBAdapter(conn);
      adapter.dropAllTables();

      for (int i = 0; i < 10; i++) {
        adapter.execute("DROP TABLE IF EXISTS tb_"+i+";");
        adapter.execute("CREATE TABLE tb_"+i+" (a INT(11));");
      }
      List<String> list = new ArrayList<>();
      ResultSet rs = adapter.executeQuery("SELECT TABLE_NAME FROM " +
          "INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" + db + "';");
      while (rs.next()) {
        list.add(rs.getString(1));
      }

      adapter.dropAllTables();

      List<String> list1 = new ArrayList<>();
      rs = adapter.executeQuery("SELECT TABLE_NAME FROM " +
          "INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" + db + "';");
      while (rs.next()) {
        list1.add(rs.getString(1));
      }
      assertEquals(10,list.size()-list1.size());
      adapter.executeUpdate("DROP DATABASE IF EXISTS abc");
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    } finally {
      if (conn != null) {
        conn.close();
      }
    }
  }*/
}
