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
package org.smartdata.server.command;

import org.junit.Assert;
import org.junit.Test;

/**
 * The tests is only about the command string translation.
 */
public class TestCommandDescriptor {

  @Test
  public void testStringToDescriptor() throws Exception {
    String cmd = "someaction arg1 -arg2 /dir/foo ; cache /testFile; action3";
    CommandDescriptor des = CommandDescriptor.fromCommandString(cmd);
    Assert.assertTrue(des.size() == 3);
    Assert.assertTrue(des.getActionName(2).equals("action3"));
    Assert.assertTrue(des.getActionArgs(2).length == 0);
  }

  @Test
  public void testTrans() throws Exception {
    CommandDescriptor des = new CommandDescriptor();
    des.addAction("action1", new String[] {"-filepath ", "/dir/foo x\""});
    des.addAction("action2", new String[] {"1", "2", "3"});
    des.addAction("action3", new String[] {"ONE_SSD", "\"2016-03-19 19:42:00\""});
    des.addAction("action4", new String[] {"-len", "123", "C:\\windows\\some.txt"});

    String cmdString = des.getCommandString();
    CommandDescriptor transDes = new CommandDescriptor(cmdString);
    Assert.assertTrue(des.size() == transDes.size());
    Assert.assertTrue(transDes.equals(des));
  }
}
