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
package org.smartdata.actions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.common.actions.ActionDescriptor;
import org.smartdata.common.message.StatusReporter;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Actions registry. Singleton.
 */
public class ActionRegistry {
  static final Logger LOG = LoggerFactory.getLogger(ActionRegistry.class);

  private static Map<String, Class> allActions = new ConcurrentHashMap<>();

  static {
    try {
      ServiceLoader<ActionFactory> actionFactories = ServiceLoader.load(ActionFactory.class);
      for (ActionFactory fact : actionFactories) {
        allActions.putAll(fact.getSupportedActions());
      }
    } catch (ServiceConfigurationError e) {
      LOG.error("Loading actions fail from factory");
    }
  }

  public static Set<String> namesOfAction() {
    return Collections.unmodifiableSet(allActions.keySet());
  }

  public static boolean checkAction(String name) {
    return allActions.containsKey(name);
  }

  public static List<ActionDescriptor> supportedActions() throws IOException {
    //TODO add more information for list ActionDescriptor
    ArrayList<ActionDescriptor> actionDescriptors = new ArrayList<>();
    for (String name : namesOfAction()) {
      actionDescriptors.add(new ActionDescriptor(name, name, "", ""));
    }
    return actionDescriptors;
  }

  public static SmartAction createAction(String name) {
    if (!checkAction(name)) {
      return null;
    }
    try {
      SmartAction smartAction = (SmartAction) allActions.get(name).newInstance();
      smartAction.setName(name);
      return smartAction;
    } catch (Exception e) {
      LOG.error("Create {} action failed", name, e);
      throw new RuntimeException("Create action failed", e);
    }
  }
}
