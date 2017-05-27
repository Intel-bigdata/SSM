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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.common.actions.ActionType;


import java.util.HashMap;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;

/**
 * Provide Action Map and Action instance
 */
public class ActionRegistry {
  static final Logger LOG = LoggerFactory.getLogger(ActionRegistry.class);

  private HashMap<String, Class> actionMap;

  public ActionRegistry() {
    actionMap = new HashMap<>();
  }

  public void initial(Configuration conf) {
    // loadNativeAction();
    try {
      // TODO read path and map from configure
      loadActions();
    } catch(Exception e) {
      LOG.error(e.getMessage());
      LOG.error("Load Native/User Actions error!");
    }
  }

  // @VisibleForTesting
  // void loadNativeAction() {
  //   // Note that native actions and ActionRegistry are in the same path
  //   String path = this.getClass().getCanonicalName();
  //   path = path.substring(0, path.lastIndexOf('.') + 1);
  //   LOG.info("Current Class Package {}", path);
  //   for (ActionType t : ActionType.values()) {
  //     String name = t.toString();
  //     try {
  //       actionMap.put(name, Class.forName(path + name));
  //     } catch (ClassNotFoundException e) {
  //       LOG.info("Class {} not found!", name);
  //       continue;
  //     }
  //   }
  // }

  @VisibleForTesting
  void loadActions() {
    try {
      ServiceLoader<Action> userActions =
          ServiceLoader.load(Action.class);
      for (Action userAction : userActions) {
        String className = userAction.getName();
        Class cl = userAction.getClass();
        actionMap.put(className, cl);
      }
    } catch (ServiceConfigurationError e) {
      LOG.error("Load Actions Fail!");
      e.printStackTrace();
    }
  }

  public boolean checkAction(String name) {
    return actionMap.containsKey(name);
  }

  public String[] namesOfAction() {
    return actionMap.keySet().toArray(new String[actionMap.size()]);
  }

  public Action newActionFromName(String name) {
    if (!checkAction(name)) {
      return null;
    }
    try {
      return (Action) actionMap.get(name).newInstance();
    } catch (Exception e) {
      LOG.info("New {} Action Error!", name);
      LOG.error(e.getMessage());
    }
    return null;
  }
}
