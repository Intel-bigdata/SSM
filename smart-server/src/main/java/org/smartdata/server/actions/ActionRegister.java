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

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.common.actions.ActionType;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.Map;

/**
 * Provide Action Map and Action instance
 */
public class ActionRegister {
  static final Logger LOG = LoggerFactory.getLogger(ActionRegister.class);

  private static ActionRegister instance = new ActionRegister();
  HashMap<String, Class> actionMap;

  synchronized public static ActionRegister getInstance() {
    return instance;
  }

  private ActionRegister() {
    actionMap = new HashMap<>();
  }

  public void initial(Configuration conf) {
    loadNativeAction();
    // TODO read from configure
    // String libPath = conf.get("libPath");
    try {
      loadUserDefinedAction(null, null);
    } catch(IOException e) {
      LOG.error(e.getMessage());
      LOG.error("Load User Actions from Path error!");
    }
  }

  public void clear() {
    actionMap.clear();
  }

  public void loadNativeAction() {
    // Note that native actions and ActionRegister are in the same path
    String path = this.getClass().getCanonicalName();
    path = path.substring(0, path.lastIndexOf('.') + 1);
    LOG.info("Current Class Package {}", path);
    for (ActionType t : ActionType.values()) {
      String name = t.toString();
      try {
        actionMap.put(name, Class.forName(path + name));
      } catch (ClassNotFoundException e) {
        LOG.info("Class {} not found!", name);
        continue;
      }
    }
  }

  public void loadUserDefinedAction(String libPath, Map<String, String> userDefinedActions) throws IOException {
    File dependencyDirectory = new File(libPath);
    File[] files = dependencyDirectory.listFiles();
    if (files == null) {
      return;
    }
    // Search this dir
    for (File jarFile: files) {
      if(jarFile.getName().contains("jar")) {
        URLClassLoader classLoader = new URLClassLoader(new URL[]{new URL("file:" +
            jarFile.getAbsolutePath())}, Thread.currentThread().getContextClassLoader());
        // Load <name, class> pair in map
        for (Map.Entry<String, String> entry : userDefinedActions.entrySet()) {
          try {
            Class userDefinedClass = classLoader.loadClass(entry.getValue());
            actionMap.put(entry.getKey(), userDefinedClass);
          } catch (ClassNotFoundException e) {
              LOG.info("Fail to oad User Actions {}!", entry.getValue());
          }
        }
      }
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
