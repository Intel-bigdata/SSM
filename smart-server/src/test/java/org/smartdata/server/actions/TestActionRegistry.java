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
package org.smartdata.server.actions;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Test ActionRegistry with native and user defined actions
 */
public class TestActionRegistry {

  // @Test
  // public void testNativeClassMap() throws Exception {
  //   ActionRegistry ar = new ActionRegistry();
  //   ar.loadNativeAction();
  //   String[] actionNames = ar.namesOfAction();
  //   Assert.assertTrue(actionNames.length == 2);
  //   Action moveFile = ar.newActionFromName("MoveFile");
  // }

 @Test
 public void testClassMap() throws Exception {
   ActionRegistry ar = new ActionRegistry();
   ar.loadActions();
   String[] actionNames = ar.namesOfAction();
   System.out.println(actionNames.length);
   Assert.assertTrue(actionNames.length == 3);
   Action ac = ar.newActionFromName("UDAction");
   ac.run();
 }

 @Test
 public void testnewActionFromName() throws Exception {
   ActionRegistry ar = new ActionRegistry();
   ar.initial(null);
   Action moveFile = ar.newActionFromName("MoveFile");
   Assert.assertTrue(moveFile.getName().contains("MoveFile"));
   Action UDAction = ar.newActionFromName("UDAction");
   Assert.assertTrue(UDAction.getName().contains("UDAction"));
 }



}
