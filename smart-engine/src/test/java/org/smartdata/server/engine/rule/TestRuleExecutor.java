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
package org.smartdata.server.engine.rule;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.smartdata.metastore.MetaStore;
import org.smartdata.metastore.TestDaoUtil;
import org.smartdata.metastore.dao.MetaStoreHelper;

import java.util.ArrayList;
import java.util.List;

public class TestRuleExecutor extends TestDaoUtil {
  private MetaStoreHelper metaStoreHelper;
  private MetaStore adapter;

  @Before
  public void initActionDao() throws Exception {
    initDao();
    metaStoreHelper = new MetaStoreHelper(druidPool.getDataSource());
    adapter = new MetaStore(druidPool);
  }

  @After
  public void closeActionDao() throws Exception {
    closeDao();
    metaStoreHelper = null;
  }

  @Test
  public void generateSQL() throws Exception {
    String countFilter = "";
    String newTable = "test";
    List<String> tableNames = new ArrayList<>();
    tableNames.add("blank_access_count_info");
    String sql;
    /*sql = "CREATE TABLE actual as SELECT fid, SUM(count)" +
        " as count FROM (SELECT * FROM blank_access_count_info " +
        "UNION ALL SELECT * FROM blank_access_count_info " +
        "UNION ALL SELECT * FROM blank_access_count_info) as tmp GROUP BY fid";
    metaStoreHelper.execute(sql);
    metaStoreHelper.dropTable("actual");*/
    // Test single element
    sql = RuleExecutor.generateSQL(tableNames, newTable, countFilter, adapter);
    try {
      metaStoreHelper.execute(sql);
      metaStoreHelper.dropTable(newTable);
    } catch (Exception e) {
      Assert.assertTrue(false);
    }
    // Test multiple elements
    tableNames.add("blank_access_count_info");
    sql = RuleExecutor.generateSQL(tableNames, newTable, countFilter, adapter);
    try {
      metaStoreHelper.execute(sql);
      metaStoreHelper.dropTable(newTable);
    } catch (Exception e) {
      Assert.assertTrue(false);
    }
  }
}
