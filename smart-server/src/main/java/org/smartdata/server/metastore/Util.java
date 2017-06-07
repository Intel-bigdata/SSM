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
package org.smartdata.server.metastore;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Utilities for table operations.
 */
public class Util {
  public static final String SQLITE_URL_PREFIX = "jdbc:sqlite:";
  public static final String MYSQL_URL_PREFIX = "jdbc:mysql:";

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
      String userName, String password) throws ClassNotFoundException, SQLException {
    Class.forName(driver);
    Connection conn = DriverManager.getConnection(url, userName, password);
    return conn;
  }

  public static Connection createSqliteConnection(String dbFilePath)
      throws ClassNotFoundException, SQLException {
    return createConnection("org.sqlite.JDBC", SQLITE_URL_PREFIX + dbFilePath,
        null, null);
  }

  public static void initializeDataBase(Connection conn) throws SQLException {
    String createEmptyTables[] = new String[] {
        "DROP TABLE IF EXISTS `access_count_tables`;",
        "DROP TABLE IF EXISTS `cached_files`;",
        "DROP TABLE IF EXISTS `ecpolicys`;",
        "DROP TABLE IF EXISTS `files`;",
        "DROP TABLE IF EXISTS `groups`;",
        "DROP TABLE IF EXISTS `owners`;",
        "DROP TABLE IF EXISTS `storages`;",
        "DROP TABLE IF EXISTS `storage_policy`;",
        "DROP TABLE IF EXISTS `xattr`;",
        "DROP TABLE IF EXISTS `rules`;",
        "DROP TABLE IF EXISTS `commands`;",
        "DROP TABLE IF EXISTS `actions`;",
        "DROP TABLE IF EXISTS `blank_access_count_info`;",  // for special cases

        "CREATE TABLE `access_count_tables` (\n" +
            "  `table_name` varchar(255) NOT NULL,\n" +
            "  `start_time` bigint(20) NOT NULL,\n" +
            "  `end_time` bigint(20) NOT NULL\n" +
            ") ;",

        "CREATE TABLE `blank_access_count_info` (\n" +
            "  `fid` bigint(20) NOT NULL,\n" +
            "  `count` bigint(20) NOT NULL\n" +
            ") ;",

        "CREATE TABLE `cached_files` (\n" +
            "  `fid` bigint(20) NOT NULL,\n" +
            "  `path` varchar(4096) NOT NULL,\n" +
            "  `from_time` bigint(20) NOT NULL,\n" +
            "  `last_access_time` bigint(20) NOT NULL,\n" +
            "  `num_accessed` int(11) NOT NULL\n" +
            ") ;",

        "CREATE TABLE `ecpolicys` (\n" +
            "  `id` INTEGER PRIMARY KEY AUTOINCREMENT,\n" +
            "  `name` varchar(255) DEFAULT NULL,\n" +
            "  `cellsize` int(11) DEFAULT NULL,\n" +
            "  `numDataUnits` int(11) DEFAULT NULL,\n" +
            "  `numParityUnits` int(11) DEFAULT NULL,\n" +
            "  `codecName` varchar(64) DEFAULT NULL\n" +
            ") ;",

        "CREATE TABLE `files` (\n" +
            "  `path` varchar(4096) NOT NULL,\n" +
            "  `fid` bigint(20) NOT NULL,\n" +
            "  `length` bigint(20) DEFAULT NULL,\n" +
            "  `block_replication` smallint(6) DEFAULT NULL,\n" +
            "  `block_size` bigint(20) DEFAULT NULL,\n" +
            "  `modification_time` bigint(20) DEFAULT NULL,\n" +
            "  `access_time` bigint(20) DEFAULT NULL,\n" +
            "  `is_dir` bit(1) DEFAULT NULL,\n" +
            "  `sid` tinyint(4) DEFAULT NULL,\n" +
            "  `oid` smallint(6) DEFAULT NULL,\n" +
            "  `gid` smallint(6) DEFAULT NULL,\n" +
            "  `permission` smallint(6) DEFAULT NULL,\n" +
            "  `ec_policy_id` smallint(6) DEFAULT NULL\n" +
            ") ;",

        "CREATE TABLE `groups` (\n" +
            "  `gid` INTEGER PRIMARY KEY AUTOINCREMENT,\n" +
            "  `group_name` varchar(255) DEFAULT NULL\n" +
            ") ;",

        "CREATE TABLE `owners` (\n" +
            "  `oid` INTEGER PRIMARY KEY AUTOINCREMENT,\n" +
            "  `owner_name` varchar(255) DEFAULT NULL\n" +
            ") ;",

        "CREATE TABLE `storages` (\n" +
            "  `type` varchar(255) NOT NULL,\n" +
            "  `capacity` bigint(20) NOT NULL,\n" +
            "  `free` bigint(20) NOT NULL\n" +
            ") ;",

        "CREATE TABLE `storage_policy` (\n" +
            "  `sid` tinyint(4) NOT NULL,\n" +
            "  `policy_name` varchar(64) DEFAULT NULL\n" +
            ") ;",

        "INSERT INTO `storage_policy` VALUES ('0', 'HOT');" ,
        "INSERT INTO `storage_policy` VALUES ('2', 'COLD');" ,
        "INSERT INTO `storage_policy` VALUES ('5', 'WARM');" ,
        "INSERT INTO `storage_policy` VALUES ('7', 'HOT');" ,
        "INSERT INTO `storage_policy` VALUES ('10', 'ONE_SSD');" ,
        "INSERT INTO `storage_policy` VALUES ('12', 'ALL_SSD');" ,
        "INSERT INTO `storage_policy` VALUES ('15', 'LAZY_PERSIST');",

        "CREATE TABLE `xattr` (\n" +
            "  `fid` bigint(20) NOT NULL,\n" +
            "  `namespace` varchar(255) NOT NULL,\n" +
            "  `name` varchar(255) NOT NULL,\n" +
            "  `value` blob NOT NULL\n" +
            ") ;",

        "CREATE TABLE `rules` (\n" +
            "  `id` INTEGER PRIMARY KEY AUTOINCREMENT,\n" +
            // TODO: may required later
            // "  `name` varchar(255) DEFAULT NULL,\n" +
            "  `state` tinyint(4) NOT NULL,\n" +
            "  `rule_text` varchar(4096) NOT NULL,\n" +
            "  `submit_time` bigint(20) NOT NULL,\n" +
            "  `last_check_time` bigint(20) DEFAULT NULL,\n" +
            "  `checked_count` int(11) NOT NULL,\n" +
            "  `commands_generated` int(11) NOT NULL\n" +
            ") ;",

        "CREATE TABLE `commands` (\n" +
            "  `cid` INTEGER PRIMARY KEY AUTOINCREMENT,\n" +
            "  `rid` INTEGER NOT NULL,\n" +
            "  `aids` varchar(4096) NOT NULL,\n" +
            "  `state` tinyint(4) NOT NULL,\n" +
            "  `parameters` varchar(4096) NOT NULL,\n" +
            "  `generate_time` bigint(20) NOT NULL,\n" +
            "  `state_changed_time` bigint(20) NOT NULL\n" +
            ") ;",

        "CREATE TABLE `actions` (\n" +
            "  `aid` INTEGER PRIMARY KEY AUTOINCREMENT,\n" +
            "  `cid` INTEGER NOT NULL,\n" +
            "  `action_name` varchar(4096) NOT NULL,\n" +
            "  `args` varchar(4096) NOT NULL,\n" +
            "  `result` varchar(4096) NOT NULL,\n" +
            "  `log` tinyint(4) NOT NULL,\n" +
            "  `successful` tinyint(4) NOT NULL,\n" +
            "  `create_time` bigint(20) NOT NULL,\n" +
            "  `finished` tinyint(4) NOT NULL,\n" +
            "  `finish_time` bigint(20) NOT NULL,\n" +
            "  `progress` INTEGER NOT NULL\n" +
            ") ;"
    };

    for (String s : createEmptyTables) {
      executeSql(conn, s);
    }
  }

  public static void executeSql(Connection conn, String sql)
      throws SQLException {
    Statement s = conn.createStatement();
    s.execute(sql);
  }

  public static boolean supportsBatchUpdates(Connection conn) {
    try {
      return conn.getMetaData().supportsBatchUpdates();
    } catch (SQLException e) {
      return false;
    }
  }
}
