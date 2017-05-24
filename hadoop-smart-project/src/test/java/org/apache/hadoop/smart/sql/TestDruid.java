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
package org.apache.hadoop.smart.sql;

import org.apache.hadoop.smart.rule.RuleInfo;
import org.apache.hadoop.smart.rule.RuleState;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.util.Properties;

public class TestDruid {

  @Test
  public void test() throws Exception {
    InputStream in = getClass().getClassLoader()
        .getResourceAsStream("druid-template.xml");
    Properties p = new Properties();
    p.loadFromXML(in);

    String dbFile = TestDBUtil.getUniqueEmptySqliteDBFile();
    String url = Util.SQLITE_URL_PREFIX + dbFile;
    p.setProperty("url", url);

    DruidPool druidPool = new DruidPool(p);
    DBAdapter adapter = new DBAdapter(druidPool);

    String rule = "file : accessCountX(10m) > 20 \n\n"
        + "and length() > 3 | cachefile";
    long submitTime = System.currentTimeMillis();
    RuleInfo info1 = new RuleInfo(0, submitTime,
        rule, RuleState.ACTIVE, 0, 0, 0);
    Assert.assertTrue(adapter.insertNewRule(info1));
    RuleInfo info1_1 = adapter.getRuleInfo(info1.getId());
    Assert.assertTrue(info1.equals(info1_1));

    long now = System.currentTimeMillis();
    adapter.updateRuleInfo(info1.getId(), RuleState.DELETED, now, 1, 1);
    RuleInfo info1_2 = adapter.getRuleInfo(info1.getId());
    Assert.assertTrue(info1_2.getLastCheckTime() == now);

    druidPool.close();
  }
}
