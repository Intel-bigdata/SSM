package org.apache.hadoop.ssm.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Utilities for table operations.
 */
public class Util {

  public static Connection createConnection(String driver, String url,
      String userName, String password) throws ClassNotFoundException, SQLException {
    Class.forName(driver);
    Connection conn = DriverManager.getConnection(url, userName, password);
    return conn;
  }

  public static Connection createSqliteConnection(String dbFilePath)
      throws ClassNotFoundException, SQLException {
    return createConnection("org.sqlite.JDBC", "jdbc:sqlite:" + dbFilePath,
        null, null);
  }

  public static void initializeDataBase(Connection conn) throws SQLException {
    String createEmptyTables[] = new String[] {
        "DROP TABLE IF EXISTS `access_count_tables`;",
        "DROP TABLE IF EXISTS `cached_files`;",
        "DROP TABLE IF EXISTS `ecpolicys`;",
        "DROP TABLE IF EXISTS `files`;",
        "DROP TABLE IF EXISTS `storages`;",
        "DROP TABLE IF EXISTS `xattr`;",
        "CREATE TABLE `access_count_tables` (\n" +
            "  `table_name` varchar(255) DEFAULT NULL,\n" +
            "  `start_time` datetime DEFAULT NULL,\n" +
            "  `end_time` datetime DEFAULT NULL\n" +
            ");",

        "CREATE TABLE `cached_files` (\n" +
            "  `fid` bigint(20) NOT NULL,\n" +
            "  `from_time` datetime DEFAULT NULL,\n" +
            "  `last_access_time` datetime DEFAULT NULL,\n" +
            "  `num_accessed` int(11) DEFAULT NULL\n" +
            ");",

        "CREATE TABLE `ecpolicys` (\n" +
            "  `id` smallint(6) DEFAULT NULL,\n" +
            "  `name` varchar(255) DEFAULT NULL,\n" +
            "  `cellsize` int(11) DEFAULT NULL,\n" +
            "  `numDataUnits` int(11) DEFAULT NULL,\n" +
            "  `numParityUnits` int(11) DEFAULT NULL,\n" +
            "  `codecName` varchar(64) DEFAULT NULL\n" +
            ");",

        "CREATE TABLE `files` (\n" +
            "  `path` varchar(4096) DEFAULT NULL,\n" +
            "  `fid` bigint(20) NOT NULL,\n" +
            "  `length` bigint(20) DEFAULT NULL,\n" +
            "  `block_replication` smallint(6) DEFAULT NULL,\n" +
            "  `block_size` bigint(20) DEFAULT NULL,\n" +
            "  `modification_time` datetime DEFAULT NULL,\n" +
            "  `access_time` datetime DEFAULT NULL,\n" +
            "  `is_dir` bit(1) DEFAULT NULL,\n" +
            "  `storage_policy` varchar(4) DEFAULT NULL,\n" +
            "  `owner` varchar(11) DEFAULT NULL,\n" +
            "  `group` varchar(11) DEFAULT NULL,\n" +
            "  `permission` smallint(6) DEFAULT NULL,\n" +
            "  `ec_policy_id` smallint(6) DEFAULT NULL\n" +
            ");",

        "CREATE TABLE `storages` (\n" +
            "  `type` varchar(255) DEFAULT NULL,\n" +
            "  `capacity` bigint(20) DEFAULT NULL,\n" +
            "  `free` bigint(20) DEFAULT NULL\n" +
            ");",

        "CREATE TABLE `xattr` (\n" +
            "  `fid` bigint(20) NOT NULL,\n" +
            "  `namespace` varchar(255) DEFAULT NULL,\n" +
            "  `name` varchar(255) DEFAULT NULL,\n" +
            "  `value` varchar(255) DEFAULT NULL\n" +
            ");"
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
