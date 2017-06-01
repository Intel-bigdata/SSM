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

import java.util.*;

/**
 * Actions registry. Singleton.
 */
public class ActionRegistry {
  static final Logger LOG = LoggerFactory.getLogger(ActionRegistry.class);

  private HashMap<String, Class> allActions;

  private static ActionRegistry theInstance = new ActionRegistry();

  public ActionRegistry() {
    allActions = new HashMap<>();
  }

  public static ActionRegistry instance() {
    synchronized (theInstance) {
      if (theInstance.allActions.isEmpty()) {
        theInstance.loadActions();
      }
    }

    return theInstance;
  }

  private void loadActions() {
    try {
      ServiceLoader<ActionFactory> actionFactories = ServiceLoader.load(ActionFactory.class);
      for (ActionFactory fact : actionFactories) {
        allActions.putAll(fact.getSupportedActions());
      }
    } catch (ServiceConfigurationError e) {
      LOG.error("Loading actions fail from factory {}", namesOfAction().getClass());
    }
  }

  public Set<String> namesOfAction() {
    return Collections.unmodifiableSet(allActions.keySet());
  }

  public boolean checkAction(String name) {
    return allActions.containsKey(name);
  }

  public SmartAction createAction(String name) {
    if (!checkAction(name)) {
      return null;
    }
    try {
      return (SmartAction) allActions.get(name).newInstance();
    } catch (Exception e) {
      LOG.error("Create {} action failed", name, e);
      throw new RuntimeException("Create action failed", e);
    }
  }
}
