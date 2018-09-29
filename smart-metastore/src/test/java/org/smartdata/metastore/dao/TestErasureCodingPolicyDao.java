/**
 * Created by qwc on 18-9-29.
 * <p>
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
import org.smartdata.metastore.TestDaoUtil;
import org.smartdata.model.ErasureCodingPolicyInfo;

import java.util.ArrayList;
import java.util.List;

public class TestErasureCodingPolicyDao extends TestDaoUtil {

  private ErasureCodingPolicyDao erasureCodingPolicyDao;

  @Before
  public void initErasureCodingPolicyDao() throws Exception {
    initDao();
    erasureCodingPolicyDao = new ErasureCodingPolicyDao(druidPool.getDataSource());
  }


  @Test
  public void testInsert() throws Exception {
    ErasureCodingPolicyInfo erasureCodingPolicyInfo = new ErasureCodingPolicyInfo((byte) 2, "info1");
    erasureCodingPolicyDao.insert(erasureCodingPolicyInfo);
    Assert.assertTrue(erasureCodingPolicyDao.getEcPolicyByName("info1").equals(erasureCodingPolicyInfo));
    Assert.assertTrue(erasureCodingPolicyDao.getEcPolicyById((byte) 2).equals(erasureCodingPolicyInfo));
  }

  @Test
  public void testInsertAll() throws Exception {
    erasureCodingPolicyDao.deleteAll();
    List<ErasureCodingPolicyInfo> list = new ArrayList<>();
    list.add(new ErasureCodingPolicyInfo((byte) 1, "info1"));
    list.add(new ErasureCodingPolicyInfo((byte) 3, "info3"));
    erasureCodingPolicyDao.insert(list);
    List<ErasureCodingPolicyInfo> getList = erasureCodingPolicyDao.getAllEcPolicies();
    Assert.assertTrue(getList.get(0).equals(list.get(0)) && getList.get(1).equals(list.get(1)));
  }

  @After
  public void closeErasureCodingPolicyDao() throws Exception {
    closeDao();
    erasureCodingPolicyDao = null;
  }
}
