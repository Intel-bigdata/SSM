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
package org.smartdata.metastore.dao;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.smartdata.model.ClusterInfo;
import org.smartdata.model.FileDiff;
import org.smartdata.model.FileDiffState;
import org.smartdata.model.FileDiffType;
import org.smartdata.metastore.utils.TestDaoUtil;

import java.util.List;

public class TestClusterInfoDao extends TestDaoUtil {
  private ClusterInfoDao clusterInfoDao;

  @Before
  public void initClusterDao() throws Exception {
    initDao();
    clusterInfoDao = new ClusterInfoDao(druidPool.getDataSource());
  }

  @After
  public void closeClusterDao() throws Exception {
    closeDao();
    clusterInfoDao = null;
  }

  @Test
  public void testInsertAndGetSingleRecord() {
    ClusterInfo clusterInfo = new ClusterInfo();
    clusterInfo.setCid(1);
    clusterInfo.setType("test");
    clusterInfo.setState("test");
    clusterInfo.setConfPath("test");
    clusterInfo.setUrl("test");
    clusterInfo.setName("test");
    clusterInfoDao.insert(clusterInfo);

    Assert.assertTrue(clusterInfoDao.getById(1).get(0).equals(clusterInfo));
  }

  @Test
  public void testBatchInssertAndQuery(){
    ClusterInfo[] clusterInfos = new ClusterInfo[2];
    clusterInfos[0] = new ClusterInfo();
    clusterInfos[0].setCid(1);
    clusterInfos[0].setType("test");
    clusterInfos[0].setState("test");
    clusterInfos[0].setConfPath("test");
    clusterInfos[0].setUrl("test");
    clusterInfos[0].setName("test");


  }
}
