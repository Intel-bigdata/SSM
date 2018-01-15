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
package org.smartdata.server.engine.cmdlet;

import org.junit.Assert;
import org.junit.Test;
import org.smartdata.model.CmdletDescriptor;

import java.util.HashMap;
import java.util.Map;

/**
 * The tests is only about the cmdlet string translation.
 */
public class TestCmdletDescriptor {

  @Test
  public void testStringToDescriptor() throws Exception {
    String cmd = "someaction -arg1 -arg2 /dir/foo ; cache -file /testFile; action3";
    CmdletDescriptor des = CmdletDescriptor.fromCmdletString(cmd);
    Assert.assertTrue(des.getActionSize() == 3);
    Assert.assertTrue(des.getActionName(2).equals("action3"));
    Assert.assertTrue(des.getActionArgs(2).size() == 0);
  }

  @Test
  public void testTrans() throws Exception {
    CmdletDescriptor des = new CmdletDescriptor();
    Map<String, String> args1 = new HashMap<>();
    args1.put("-filePath", "/dir/foo x");
    args1.put("-len", "100");

    Map<String, String> args2 = new HashMap<>();
    args2.put("-version", "");

    Map<String, String> args3 = new HashMap<>();
    args3.put("-storage", "ONE_SSD");
    args3.put("-time", "2016-03-19 19:42:00");

    des.addAction("action1", args1);
    des.addAction("action2", args2);
    des.addAction("action3", args3);

    String cmdString = des.getCmdletString();
    CmdletDescriptor transDes = new CmdletDescriptor(cmdString);
    Assert.assertTrue(des.getActionSize() == transDes.getActionSize());
    Assert.assertTrue(transDes.equals(des));
  }
}
