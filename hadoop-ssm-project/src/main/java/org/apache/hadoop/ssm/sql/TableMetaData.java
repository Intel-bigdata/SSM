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
package org.apache.hadoop.ssm.sql;

import java.util.HashMap;
import java.util.Map;

/**
 * Info, constraints and relations about tables.
 */
public class TableMetaData {
  private static Map<String, String[]> mapJoinableKeys = new HashMap<>();
  private static Map<String, String[]> mapTableColumns = new HashMap<>();

  static {
    mapJoinableKeys.clear();
    mapJoinableKeys.put("files-access_count_tables",
        new String[] {"fid", "fid"});
    mapJoinableKeys.put("files-cached_files",
        new String[] {"fid", "fid"});
    mapJoinableKeys.put("files-groups",
        new String[] {"gid", "gid"});
    mapJoinableKeys.put("files-owners",
        new String[] {"oid", "oid"});
    // TODO: others

    // TODO: hard code them now
    mapTableColumns.clear();
    mapTableColumns.put("files", new String[] {
        "path", "fid"
    });
    mapTableColumns.put("storages", new String[] {
        "type", "capacity", "free"
    });
    // TODO: add other tables
  }

  public static String[] getJoinableKey(String tableA, String tableB) {
    String key = tableA + "-" + tableB;
    if (mapJoinableKeys.containsKey(key)) {
      return mapJoinableKeys.get(key).clone();
    }

    key = tableB + "-" + tableA;
    if (mapJoinableKeys.containsKey(key)) {
      String[] result = mapJoinableKeys.get(key);
      String[] ret = new String[] {result[1], result[0]};
      return ret;
    }
    return null;
  }

  public static String[] getTableColumns(String tableName) {
    if (mapTableColumns.containsKey(tableName)) {
      return mapTableColumns.get(tableName).clone();
    }
    return null;
  }
}
