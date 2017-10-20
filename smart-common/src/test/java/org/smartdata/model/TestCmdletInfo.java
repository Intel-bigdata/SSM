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

import java.util.Random;


public class TestCmdletInfo {
  @Test
  public void testEquals() throws Exception {
    //Case 1
    Assert.assertEquals(true, new CmdletInfo().equals(new CmdletInfo()));

    //Case 2
    Random random = new Random();
    CmdletInfo cmdletInfo =
        new CmdletInfo(
            random.nextLong(),
            random.nextLong(),
            CmdletState.NOTINITED,
            " ",
            random.nextLong(),
            random.nextLong());
    Assert.assertEquals(true, cmdletInfo.equals(cmdletInfo));

    //Case 3
    Assert.assertEquals(false, cmdletInfo.equals(new Object()));
    Assert.assertEquals(false, new Object().equals(cmdletInfo));

    //Case 4
    Assert.assertEquals(false, cmdletInfo.equals(null));

    //Case 5
    CmdletInfo cmdletInfo1 = new CmdletInfo(1, 1, CmdletState.CANCELLED, "test", 1, 1);
    CmdletInfo cmdletInfo2 = new CmdletInfo(1, 1, CmdletState.CANCELLED, "test", 1, 1);
    Assert.assertEquals(true, cmdletInfo1.equals(cmdletInfo2));

    //Case 6
    cmdletInfo1 = new CmdletInfo(1, 1, CmdletState.CANCELLED, null, 1, 1);
    cmdletInfo2 = new CmdletInfo(1, 1, CmdletState.CANCELLED, "test", 1, 1);
    Assert.assertEquals(false, cmdletInfo1.equals(cmdletInfo2));
    Assert.assertEquals(false, cmdletInfo2.equals(cmdletInfo1));

    //Case 7
    cmdletInfo1 = new CmdletInfo(1, 1, CmdletState.CANCELLED, null, 1, 1);
    cmdletInfo2 = new CmdletInfo(1, 1, CmdletState.CANCELLED, null, 1, 1);
    Assert.assertEquals(true, cmdletInfo1.equals(cmdletInfo2));

    //Case 8
    cmdletInfo1 = new CmdletInfo(1, 1, null, "test", 1, 1);
    cmdletInfo2 = new CmdletInfo(1, 1, CmdletState.CANCELLED, "test", 1, 1);
    Assert.assertEquals(false, cmdletInfo1.equals(cmdletInfo2));

    //Case 9
    cmdletInfo1 = new CmdletInfo(1, 1, null, "test", 1, 1);
    cmdletInfo2 = new CmdletInfo(1, 1, null, "test", 1, 1);
    Assert.assertEquals(true, cmdletInfo1.equals(cmdletInfo2));
  }
}
