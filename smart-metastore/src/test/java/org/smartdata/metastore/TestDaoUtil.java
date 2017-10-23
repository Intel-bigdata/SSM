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

import org.smartdata.metastore.utils.MetaStoreUtils;

import java.io.File;
import java.io.InputStream;
import java.util.Properties;


public class TestDaoUtil {
  protected DruidPool druidPool;
  protected String dbFile;
  protected String url;

  public void initDao() throws Exception {
    InputStream in = getClass().getClassLoader()
        .getResourceAsStream("druid-template.xml");
    Properties p = new Properties();
    p.loadFromXML(in);

    dbFile = TestDBUtil.getUniqueEmptySqliteDBFile();
    url = MetaStoreUtils.SQLITE_URL_PREFIX + dbFile;
    p.setProperty("url", url);

    druidPool = new DruidPool(p);
  }

  public void closeDao() throws Exception {
    File db = new File(dbFile);
    if (db.exists()) {
      db.delete();
    }
    if (druidPool != null) {
      druidPool.close();
    }
  }
}
