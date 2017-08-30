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
package org.smartdata.model.rule;

import java.util.ArrayList;
import java.util.List;

public class RuleExecutorPluginManager {
  private static final RuleExecutorPluginManager inst = new RuleExecutorPluginManager();

  private static List<RuleExecutorPlugin> plugins = new ArrayList<>();

  private RuleExecutorPluginManager() {
  }

  public static RuleExecutorPluginManager getInstance() {
    return inst;
  }

  public synchronized static void addPlugin(RuleExecutorPlugin plugin) {
    if (!plugins.contains(plugin)) {
      plugins.add(plugin);
    }
  }

  public synchronized static void deletePlugin(RuleExecutorPlugin plugin) {
    if (plugins.contains(plugin)) {
      plugins.remove(plugin);
    }
  }

  public static List<RuleExecutorPlugin> getPlugins() {
    List<RuleExecutorPlugin> copy = new ArrayList<>();
    copy.addAll(plugins);
    return copy;
  }
}
