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
package org.smartdata.action;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.action.annotation.ActionSignature;
import org.smartdata.model.ActionDescriptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Actions registry. Singleton.
 */
public class ActionRegistry {
  static final Logger LOG = LoggerFactory.getLogger(ActionRegistry.class);
  private static Map<String, Class<? extends SmartAction>> allActions = new ConcurrentHashMap<>();

  static {
    try {
      ServiceLoader<ActionFactory> actionFactories = ServiceLoader.load(ActionFactory.class);
      for (ActionFactory fact : actionFactories) {
        allActions.putAll(fact.getSupportedActions());
      }
    } catch (ServiceConfigurationError e) {
      LOG.error("Load actions failed from factory");
    }
  }

  public static Set<String> registeredActions() {
    return Collections.unmodifiableSet(allActions.keySet());
  }

  public static boolean registeredAction(String name) {
    return allActions.containsKey(name);
  }

  public static List<ActionDescriptor> supportedActions() throws IOException {
    ArrayList<ActionDescriptor> actionDescriptors = new ArrayList<>();
    for (Class<? extends SmartAction> clazz : allActions.values()) {
      ActionSignature signature = clazz.getAnnotation(ActionSignature.class);
      if (signature != null) {
        actionDescriptors.add(fromSignature(signature));
      }
    }
    return actionDescriptors;
  }

  public static SmartAction createAction(String name) throws ActionException {
    if (!registeredAction(name)) {
      throw new ActionException("Unregistered action " + name);
    }
    try {
      SmartAction smartAction = allActions.get(name).newInstance();
      smartAction.setName(name);
      return smartAction;
    } catch (Exception e) {
      LOG.error("Create {} action failed", name, e);
      throw new ActionException(e);
    }
  }

  private static ActionDescriptor fromSignature(ActionSignature signature) {
    return new ActionDescriptor(
        signature.actionId(), signature.displayName(), signature.usage(), signature.description());
  }
}
