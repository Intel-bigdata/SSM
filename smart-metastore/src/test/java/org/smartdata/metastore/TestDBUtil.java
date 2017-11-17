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
package org.smartdata.metastore;

import org.smartdata.metastore.utils.MetaStoreUtils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.Connection;
import java.util.UUID;

/**
 * Utilities for accessing the testing database.
 */
public class TestDBUtil {

  /**
   * Get a connect to the testing database. A new physical database
   * file for each call.
   *
   * @return
   */
  public static Connection getTestDBInstance()
      throws MetaStoreException {
    // TODO remove today
    String srcdir = System.getProperty("srcdir",
        System.getProperty("user.dir") + "/src/main/resources");
    String srcPath = srcdir + "/data-schema.db";
    String destPath = getUniqueDBFilePath();
    copyFile(srcPath, destPath);
    Connection conn = MetaStoreUtils.createSqliteConnection(destPath);
    return conn;
  }

  public static String getTestDir() {
    String testdir = System.getProperty("testdir",
        System.getProperty("user.dir") + "/target/test-dir");
    return testdir;
  }

  public static String getUniqueFilePath() {
    return getTestDir() + "/" + UUID.randomUUID().toString() + System.currentTimeMillis();
  }

  public static String getUniqueDBFilePath() {
    return getUniqueFilePath() + ".db";
  }

  public static Connection getUniqueEmptySqliteDBInstance()
      throws MetaStoreException {
    return MetaStoreUtils.createSqliteConnection(getUniqueEmptySqliteDBFile());
  }

  /**
   * Get an initialized empty Sqlite database file path.
   *
   * @return
   * @throws IOException
   * @throws MetaStoreException
   * @throws ClassNotFoundException
   */
  public static String getUniqueEmptySqliteDBFile()
      throws MetaStoreException {
    String dbFile = getUniqueDBFilePath();
    Connection conn = null;
    try {
      conn = MetaStoreUtils.createSqliteConnection(dbFile);
      MetaStoreUtils.initializeDataBase(conn);
      conn.close();
      return dbFile;
    } catch (Exception e) {
      throw new MetaStoreException(e);
    } finally {
      if (conn != null) {
        try {
          conn.close();
        } catch (Exception e) {
          throw new MetaStoreException(e);
        }
      }
      File file = new File(dbFile);
      file.deleteOnExit();
    }
  }

  public static boolean copyFile(String srcPath, String destPath) {
    boolean flag = false;
    File src = new File(srcPath);
    if (!src.exists()) {
      System.out.println("source file:" + srcPath + "not exist");
      return false;
    }
    File dest = new File(destPath);
    if (dest.exists()) {
      dest.delete();
    } else {
      if (!dest.getParentFile().exists()) {
        if (!dest.getParentFile().mkdirs()) {
          return false;
        }
      }
    }

    BufferedInputStream in = null;
    PrintStream out = null;

    try {
      in = new BufferedInputStream(new FileInputStream(src));
      out = new PrintStream(
          new BufferedOutputStream(
              new FileOutputStream(dest)));

      byte[] buffer = new byte[1024 * 100];
      int len = -1;
      while ((len = in.read(buffer)) != -1) {
        out.write(buffer, 0, len);
      }
      dest.deleteOnExit();
      return true;
    } catch (Exception e) {
      System.out.println("copying failed" + e.getMessage());
      flag = true;
      return false;
    } finally {
      try {
        in.close();
        out.close();
        if (flag) {
          dest.delete();
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
}
