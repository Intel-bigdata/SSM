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

import org.junit.Assert;
import org.junit.Test;
import org.smartdata.metastore.utils.MetaStoreUtils;
import org.smartdata.model.RuleInfo;
import org.smartdata.model.RuleState;

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
    String url = MetaStoreUtils.SQLITE_URL_PREFIX + dbFile;
    p.setProperty("url", url);

    DruidPool druidPool = new DruidPool(p);
    MetaStore adapter = new MetaStore(druidPool);

    String rule = "file : accessCount(10m) > 20 \n\n"
        + "and length() > 3 | cache";
    long submitTime = System.currentTimeMillis();
    RuleInfo info1 = new RuleInfo(0, submitTime,
        rule, RuleState.ACTIVE, 0, 0, 0);
    Assert.assertTrue(adapter.insertNewRule(info1));
    RuleInfo info11 = adapter.getRuleInfo(info1.getId());
    Assert.assertTrue(info1.equals(info11));

    long now = System.currentTimeMillis();
    adapter.updateRuleInfo(info1.getId(), RuleState.DELETED, now, 1, 1);
    RuleInfo info12 = adapter.getRuleInfo(info1.getId());
    Assert.assertTrue(info12.getLastCheckTime() == now);

    druidPool.close();
  }
}
