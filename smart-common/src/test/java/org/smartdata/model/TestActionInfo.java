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
package org.smartdata.model;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Random;

/**
 * Created by zhendu on 2017/7/5.
 */
public class TestActionInfo {
  private class ChildTestActionInfo extends ActionInfo {

  }

  @Test
  public void testEquals() throws Exception {
    //Case 1
    Assert.assertEquals(true, new ActionInfo().equals(new ActionInfo()));

    //Case 2
    Random random = new Random();
    ActionInfo actionInfo = new ActionInfo(random.nextLong(), random.nextLong(),
            " ", new HashMap<String, String>(), " ", " ",
            random.nextBoolean(), random.nextLong(), random.nextBoolean(),
            random.nextLong(), random.nextFloat());
    Assert.assertEquals(true, actionInfo.equals(actionInfo));

    //Case 3
    Assert.assertEquals(false, actionInfo.equals(new ChildTestActionInfo()));
    Assert.assertEquals(false, new ChildTestActionInfo().equals(actionInfo));

    //Case4
    Assert.assertEquals(false, actionInfo.equals(null));

    //Case5
    ActionInfo actionInfo1 = new ActionInfo(1, 1,
            "test", new HashMap<String, String>(), "test", "test",
            true, 1, true,
            1, 1.1f);
    ActionInfo actionInfo2 = new ActionInfo(1, 1,
            "test", new HashMap<String, String>(), "test", "test",
            true, 1, true,
            1, 1.1f);
    Assert.assertEquals(true, actionInfo1.equals(actionInfo2));


    //Case6
    actionInfo1 = new ActionInfo(1, 1,
            "test", new HashMap<String, String>(), "test", "test",
            true, 1, true,
            1, 1.1f);
    actionInfo2 = new ActionInfo(1, 1,
            "test", new HashMap<String, String>(), "", "test",
            true, 1, true,
            1, 1.1f);
    Assert.assertEquals(false, actionInfo1.equals(actionInfo2));

    //Case7
    actionInfo1 = new ActionInfo(1, 1,
            "test", new HashMap<String, String>(), null, "test",
            true, 1, true,
            1, 1.1f);
    actionInfo2 = new ActionInfo(1, 1,
            "test", new HashMap<String, String>(), null, "test",
            true, 1, true,
            1, 1.1f);
    Assert.assertEquals(true, actionInfo1.equals(actionInfo2));

    //Case8
    actionInfo1 = new ActionInfo(1, 1,
            "test", new HashMap<String, String>(), null, "test",
            true, 1, true,
            1, 1.1f);
    actionInfo2 = new ActionInfo(1, 1,
            "test", new HashMap<String, String>(), " ", "test",
            true, 1, true,
            1, 1.1f);
    Assert.assertEquals(false, actionInfo1.equals(actionInfo2));

    //Case9
    actionInfo1 = new ActionInfo(1, 1,
            "test", new HashMap<String, String>(), "", "test",
            true, 1, true,
            1, 1.1f);
    actionInfo2 = new ActionInfo(1, 1,
            "test", null, " ", "test",
            true, 1, true,
            1, 1.1f);
    Assert.assertEquals(false, actionInfo1.equals(actionInfo2));
    Assert.assertEquals(false, actionInfo2.equals(actionInfo1));

  }
}
