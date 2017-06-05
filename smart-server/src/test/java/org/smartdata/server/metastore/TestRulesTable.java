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

import org.junit.Assert;
import org.junit.Test;
import org.smartdata.common.rule.RuleInfo;
import org.smartdata.common.rule.RuleState;
import org.smartdata.server.metastore.DBAdapter;
import org.smartdata.server.metastore.Util;

import java.io.File;
import java.sql.Connection;
import java.util.List;

/**
 * Tests for table 'rules'.
 */
public class TestRulesTable {

  /**
   * Insert rules into table and retrieve them back.
   * @throws Exception
   */
  @Test
  public void testRuleInsert() throws Exception {
    String dbFile = TestDBUtil.getUniqueDBFilePath();
    Connection conn = null;
    try {
      conn = Util.createSqliteConnection(dbFile);
      Util.initializeDataBase(conn);
      String rule = "file : accessCountX(10m) > 20 \n\n"
          + "and length() > 3 | cache";
      long submitTime = System.currentTimeMillis();
      RuleInfo info1 = new RuleInfo(0, submitTime,
          rule, RuleState.ACTIVE, 0, 0, 0);
      DBAdapter adapter = new DBAdapter(conn);
      Assert.assertTrue(adapter.insertNewRule(info1));
      RuleInfo info1_1 = adapter.getRuleInfo(info1.getId());
      Assert.assertTrue(info1.equals(info1_1));

      RuleInfo info2 = new RuleInfo(0, submitTime,
          rule, RuleState.ACTIVE, 0, 0, 0);
      Assert.assertTrue(adapter.insertNewRule(info2));
      RuleInfo info2_1 = adapter.getRuleInfo(info2.getId());
      Assert.assertFalse(info1_1.equals(info2_1));

      List<RuleInfo> infos = adapter.getRuleInfo();
      assert(infos.size() == 2);

    } finally {
      if (conn != null) {
        conn.close();
      }
      File file = new File(dbFile);
      file.deleteOnExit();
    }
  }
}
