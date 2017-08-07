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
package org.smartdata.hdfs.action;

import org.junit.Assert;
import org.junit.Test;
import org.smartdata.actions.ActionException;
import org.smartdata.actions.ActionRegistry;

import java.io.IOException;
import java.util.Set;

public class TestActionRegistry {

  @Test
  public void testInit() throws IOException {
    Set<String> actionNames = ActionRegistry.registeredActions();
    // System.out.print(actionNames.size());
    Assert.assertTrue(actionNames.size() > 0);
  }

  @Test
  public void testCreateAction() throws IOException, ActionException {
    Assert.assertTrue(ActionRegistry.createAction("cache") instanceof CacheFileAction);
    Set<String> actionNames = ActionRegistry.registeredActions();
    // create all kinds of actions
    for (String name : actionNames) {
      ActionRegistry.createAction(name);
    }
  }
}
