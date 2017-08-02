/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.smartdata.metastore.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.conf.SmartConf;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.metastore.DruidPool;
import org.smartdata.metastore.MetaStore;
import org.smartdata.metastore.MetaStoreException;

import java.io.File;
import java.io.FileInputStream;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Utilities for table operations.
 */
public class MetaStoreUtils {
  public static final String SQLITE_URL_PREFIX = "jdbc:sqlite:";
  public static final String MYSQL_URL_PREFIX = "jdbc:mysql:";
  static final Logger LOG = LoggerFactory.getLogger(MetaStoreUtils.class);

  public static Connection createConnection(String url,
      String userName, String password)
      throws ClassNotFoundException, SQLException {
    if (url.startsWith(SQLITE_URL_PREFIX)) {
      Class.forName("org.sqlite.JDBC");
    } else if (url.startsWith(MYSQL_URL_PREFIX)) {
      Class.forName("com.mysql.jdbc.Driver");
    }
    Connection conn = DriverManager.getConnection(url, userName, password);
    return conn;
  }

  public static Connection createConnection(String driver, String url,
      String userName,
      String password) throws ClassNotFoundException, SQLException {
    Class.forName(driver);
    Connection conn = DriverManager.getConnection(url, userName, password);
    return conn;
  }

  public static Connection createSqliteConnection(String dbFilePath)
      throws MetaStoreException {
    try {
      return createConnection("org.sqlite.JDBC", SQLITE_URL_PREFIX + dbFilePath,
          null, null);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public static void initializeDataBase(
      Connection conn) throws MetaStoreException {
    String createEmptyTables[] = new String[]{
        "DROP TABLE IF EXISTS access_count_tables;",
        "DROP TABLE IF EXISTS cached_files;",
        "DROP TABLE IF EXISTS ecpolicys;",
        "DROP TABLE IF EXISTS files;",
        "DROP TABLE IF EXISTS groups;",
        "DROP TABLE IF EXISTS owners;",
        "DROP TABLE IF EXISTS storages;",
        "DROP TABLE IF EXISTS storage_policy;",
        "DROP TABLE IF EXISTS xattr;",
        "DROP TABLE IF EXISTS datanode_info;",
        "DROP TABLE IF EXISTS datanode_storage_info;",
        "DROP TABLE IF EXISTS rules;",
        "DROP TABLE IF EXISTS cmdlets;",
        "DROP TABLE IF EXISTS actions;",
        "DROP TABLE IF EXISTS blank_access_count_info;",  // for special cases
        "DROP TABLE IF EXISTS file_diff;",  // incremental diff for disaster recovery

        "CREATE TABLE access_count_tables (\n" +
            "  table_name varchar(255) NOT NULL,\n" +
            "  start_time bigint(20) NOT NULL,\n" +
            "  end_time bigint(20) NOT NULL\n" +
            ") ;",

        "CREATE TABLE blank_access_count_info (\n" +
            "  fid bigint(20) NOT NULL,\n" +
            "  count bigint(20) NOT NULL\n" +
            ") ;",

        "CREATE TABLE cached_files (\n" +
            "  fid bigint(20) NOT NULL,\n" +
            "  path varchar(4096) NOT NULL,\n" +
            "  from_time bigint(20) NOT NULL,\n" +
            "  last_access_time bigint(20) NOT NULL,\n" +
            "  num_accessed int(11) NOT NULL\n" +
            ") ;",

        "CREATE TABLE ecpolicys (\n" +
            "  id INTEGER PRIMARY KEY AUTOINCREMENT,\n" +
            "  name varchar(255) DEFAULT NULL,\n" +
            "  cellsize int(11) DEFAULT NULL,\n" +
            "  numDataUnits int(11) DEFAULT NULL,\n" +
            "  numParityUnits int(11) DEFAULT NULL,\n" +
            "  codecName varchar(64) DEFAULT NULL\n" +
            ") ;",

        "CREATE TABLE files (\n" +
            "  path varchar(4096) NOT NULL,\n" +
            "  fid bigint(20) NOT NULL,\n" +
            "  length bigint(20) DEFAULT NULL,\n" +
            "  block_replication smallint(6) DEFAULT NULL,\n" +
            "  block_size bigint(20) DEFAULT NULL,\n" +
            "  modification_time bigint(20) DEFAULT NULL,\n" +
            "  access_time bigint(20) DEFAULT NULL,\n" +
            "  is_dir tinyint(1) DEFAULT NULL,\n" +
            "  sid tinyint(4) DEFAULT NULL,\n" +
            "  oid smallint(6) DEFAULT NULL,\n" +
            "  gid smallint(6) DEFAULT NULL,\n" +
            "  permission smallint(6) DEFAULT NULL,\n" +
            "  ec_policy_id smallint(6) DEFAULT NULL\n" +
            ") ;",

        "CREATE TABLE groups (\n" +
            "  gid INTEGER PRIMARY KEY AUTOINCREMENT,\n" +
            "  group_name varchar(255) DEFAULT NULL\n" +
            ") ;",

        "CREATE TABLE owners (\n" +
            "  oid INTEGER PRIMARY KEY AUTOINCREMENT,\n" +
            "  owner_name varchar(255) DEFAULT NULL\n" +
            ") ;",

        "CREATE TABLE storages (\n" +
            "  type varchar(255) NOT NULL,\n" +
            "  capacity bigint(20) NOT NULL,\n" +
            "  free bigint(20) NOT NULL\n" +
            ") ;",

        "CREATE TABLE storage_policy (\n" +
            "  sid tinyint(4) NOT NULL,\n" +
            "  policy_name varchar(64) DEFAULT NULL\n" +
            ") ;",

        "INSERT INTO storage_policy VALUES ('0', 'HOT');",
        "INSERT INTO storage_policy VALUES ('2', 'COLD');",
        "INSERT INTO storage_policy VALUES ('5', 'WARM');",
        "INSERT INTO storage_policy VALUES ('7', 'HOT');",
        "INSERT INTO storage_policy VALUES ('10', 'ONE_SSD');",
        "INSERT INTO storage_policy VALUES ('12', 'ALL_SSD');",
        "INSERT INTO storage_policy VALUES ('15', 'LAZY_PERSIST');",

        "CREATE TABLE xattr (\n" +
            "  fid bigint(20) NOT NULL,\n" +
            "  namespace varchar(255) NOT NULL,\n" +
            "  name varchar(255) NOT NULL,\n" +
            "  value blob NOT NULL\n" +
            ") ;",

        "CREATE TABLE datanode_info (\n" +
            "  uuid varchar(64) NOT NULL,\n" +
            "  hostname varchar(255) NOT NULL,\n" +   // DatanodeInfo
            "  ip varchar(16) DEFAULT NULL,\n" +
            "  port tinyint(4) DEFAULT NULL\n" +
            "  cache_capacity bigint(20) DEFAULT NULL,\n" +
            "  cache_used bigint(20) DEFAULT NULL,\n" +
            "  location varchar(255) DEFAULT NULL,\n" +
            ") ;",

        "CREATE TABLE datanode_storage_info (\n" +
            "  uuid varchar(64) NOT NULL,\n" +
            "  sid tinyint(4) NOT NULL,\n" +          // storage type
            "  state tinyint(4) NOT NULL,\n" +        // DatanodeStorage.state
            "  storageid varchar(64) NOT NULL,\n" +   // StorageReport ...
            "  failed tinyint(1) DEFAULT NULL,\n" +
            "  capacity bigint(20) DEFAULT NULL,\n" +
            "  dfs_used bigint(20) DEFAULT NULL,\n" +
            "  remaining bigint(20) DEFAULT NULL,\n" +
            "  block_pool_used bigint(20) DEFAULT NULL,\n" +
            ") ;",

        "CREATE TABLE rules (\n" +
            "  id INTEGER PRIMARY KEY AUTOINCREMENT,\n" +
            // TODO: may required later
            // "  name varchar(255) DEFAULT NULL,\n" +
            "  state tinyint(4) NOT NULL,\n" +
            "  rule_text varchar(4096) NOT NULL,\n" +
            "  submit_time bigint(20) NOT NULL,\n" +
            "  last_check_time bigint(20) DEFAULT NULL,\n" +
            "  checked_count int(11) NOT NULL,\n" +
            "  cmdlets_generated int(11) NOT NULL\n" +
            ") ;",

        "CREATE TABLE cmdlets (\n" +
            "  cid INTEGER PRIMARY KEY,\n" +
            "  rid INTEGER NOT NULL,\n" +
            "  aids varchar(4096) NOT NULL,\n" +
            "  state tinyint(4) NOT NULL,\n" +
            "  parameters varchar(4096) NOT NULL,\n" +
            "  generate_time bigint(20) NOT NULL,\n" +
            "  state_changed_time bigint(20) NOT NULL\n" +
            ") ;",

        "CREATE TABLE actions (\n" +
            "  aid INTEGER PRIMARY KEY,\n" +
            "  cid INTEGER NOT NULL,\n" +
            "  action_name varchar(4096) NOT NULL,\n" +
            "  args varchar(4096) NOT NULL,\n" +
            "  result text NOT NULL,\n" +
            "  log text NOT NULL,\n" +
            "  successful tinyint(4) NOT NULL,\n" +
            "  create_time bigint(20) NOT NULL,\n" +
            "  finished tinyint(4) NOT NULL,\n" +
            "  finish_time bigint(20) NOT NULL,\n" +
            "  progress INTEGER NOT NULL\n" +
            ") ;",

        "CREATE TABLE file_diff (\n" +
            "  did INTEGER PRIMARY KEY AUTOINCREMENT,\n" +
            "  diff_type varchar(4096) NOT NULL,\n" +
            "  parameters varchar(4096) NOT NULL,\n" +
            "  applied tinyint(4) NOT NULL,\n" +
            "  create_time bigint(20) NOT NULL\n" +
            ") ;"
    };
    try {
      for (String s : createEmptyTables) {
        String url = conn.getMetaData().getURL();
        if (url.startsWith(MetaStoreUtils.MYSQL_URL_PREFIX)) {
          s = s.replace("AUTOINCREMENT", "AUTO_INCREMENT");
        }
        executeSql(conn, s);
      }
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public static void executeSql(Connection conn, String sql)
      throws MetaStoreException {
    try {
      Statement s = conn.createStatement();
      s.execute(sql);
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public static boolean supportsBatchUpdates(Connection conn) {
    try {
      return conn.getMetaData().supportsBatchUpdates();
    } catch (Exception e) {
      return false;
    }
  }

  public static void formatDatabase(SmartConf conf) throws MetaStoreException {
    getDBAdapter(conf).formatDataBase();
  }

  public static MetaStore getDBAdapter(
      SmartConf conf) throws MetaStoreException {
    URL pathUrl = ClassLoader.getSystemResource("");
    String path = pathUrl.getPath();

    String fileName = "druid.xml";
    String expectedCpPath = path + fileName;
    LOG.info("Expected DB connection pool configuration path = "
        + expectedCpPath);
    File cpConfigFile = new File(expectedCpPath);
    if (cpConfigFile.exists()) {
      LOG.info("Using pool configure file: " + expectedCpPath);
      Properties p = new Properties();
      try {
        p.loadFromXML(new FileInputStream(cpConfigFile));

        String url = conf.get(SmartConfKeys.SMART_METASTORE_DB_URL_KEY);
        if (url != null) {
          p.setProperty("url", url);
        }

        String purl = p.getProperty("url");
        if (purl == null || purl.length() == 0) {
          purl = getDefaultSqliteDB(); // For testing
          p.setProperty("url", purl);
          LOG.warn("Database URL not specified, using " + purl);
        }

        for (String key : p.stringPropertyNames()) {
          LOG.info("\t" + key + " = " + p.getProperty(key));
        }
        return new MetaStore(new DruidPool(p));
      } catch (Exception e) {
        throw new MetaStoreException(e);
      }
    } else {
      LOG.info("DB connection pool config file " + expectedCpPath
          + " NOT found.");
    }
    // Get Default configure from druid-template.xml
    fileName = "druid-template.xml";
    expectedCpPath = path + fileName;
    LOG.info("Expected DB connection pool configuration path = "
        + expectedCpPath);
    cpConfigFile = new File(expectedCpPath);
    LOG.info("Using pool configure file: " + expectedCpPath);
    Properties p = new Properties();
    try {
      p.loadFromXML(new FileInputStream(cpConfigFile));
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
    String url = conf.get(SmartConfKeys.SMART_METASTORE_DB_URL_KEY);
    if (url != null) {
      p.setProperty("url", url);
    }
    for (String key : p.stringPropertyNames()) {
      LOG.info("\t" + key + " = " + p.getProperty(key));
    }
    return new MetaStore(new DruidPool(p));
  }

  public static Integer getKey(Map<Integer, String> map, String value) {
    for (Integer key : map.keySet()) {
      if (map.get(key).equals(value)) {
        return key;
      }
    }
    return null;
  }

  /**
   * This default behavior provided here is mainly for convenience.
   *
   * @return
   */
  private static String getDefaultSqliteDB() throws MetaStoreException {
    String absFilePath = System.getProperty("user.home")
        + "/smart-test-default.db";
    File file = new File(absFilePath);
    if (file.exists()) {
      return MetaStoreUtils.SQLITE_URL_PREFIX + absFilePath;
    }
    try {
      Connection conn = MetaStoreUtils.createSqliteConnection(absFilePath);
      MetaStoreUtils.initializeDataBase(conn);
      conn.close();
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
    return MetaStoreUtils.SQLITE_URL_PREFIX + absFilePath;
  }

  public static void dropAllTablesSqlite(
      Connection conn) throws MetaStoreException {
    try {
      Statement s = conn.createStatement();
      ResultSet rs = s.executeQuery("SELECT tbl_name FROM sqlite_master;");
      List<String> list = new ArrayList<>();
      while (rs.next()) {
        list.add(rs.getString(1));
      }
      for (String tb : list) {
        if (!"sqlite_sequence".equals(tb)) {
          s.execute("DROP TABLE IF EXISTS '" + tb + "';");
        }
      }
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }

  public static void dropAllTablesMysql(Connection conn,
      String url) throws MetaStoreException {
    try {
      Statement stat = conn.createStatement();
      String dbName;
      if (url.contains("?")) {
        dbName = url.substring(url.indexOf("/", 13) + 1, url.indexOf("?"));
      } else {
        dbName = url.substring(url.lastIndexOf("/") + 1, url.length());
      }
      LOG.info("Drop All tables of Current DBname: " + dbName);
      ResultSet rs = stat.executeQuery("SELECT TABLE_NAME FROM "
          + "INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" + dbName + "';");
      List<String> tbList = new ArrayList<>();
      while (rs.next()) {
        tbList.add(rs.getString(1));
      }
      for (String tb : tbList) {
        LOG.info(tb);
        stat.execute("DROP TABLE IF EXISTS " + tb + ";");
      }
    } catch (Exception e) {
      throw new MetaStoreException(e);
    }
  }
}
