/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.smartdata.agent;

import org.smartdata.actions.ActionFactory;
import org.smartdata.actions.SmartAction;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MockedActionFactory implements ActionFactory {

  public static final String actionName = "mockAction";
  public static final Map<String, String> actionArgs = new HashMap<>();
  public static final SmartAction mockAction = mock(SmartAction.class);

  static {
    when(mockAction.getName()).thenReturn(actionName);
    when(mockAction.getArguments()).thenReturn(actionArgs);
  }

  @Override
  public Map<String, Class<? extends SmartAction>> getSupportedActions() {
    Map<String, Class<? extends SmartAction>> actions = new HashMap<>();
    actions.put(actionName, mockAction.getClass());
    return actions;
  }
}
