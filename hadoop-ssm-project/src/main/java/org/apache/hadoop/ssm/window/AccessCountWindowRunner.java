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
package org.apache.hadoop.ssm.window;

import com.gs.collections.impl.map.mutable.UnifiedMap;
import com.gs.collections.impl.map.sorted.mutable.TreeSortedMap;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class AccessCountWindowRunner<K> {
  private UnifiedMap<K, TreeSortedMap<Window, Integer>> windowInputs;
  private WindowAssigner windowAssigner;

  public AccessCountWindowRunner(WindowAssigner windowAssigner) {
    this.windowAssigner = windowAssigner;
    this.windowInputs = new UnifiedMap<>();
  }

  public void process(K key, Window window, Integer value) {
    if (!this.windowInputs.containsKey(key)) {
      this.windowInputs.put(key, new TreeSortedMap<>());
    }
    TreeSortedMap<Window, Integer> windows = this.windowInputs.get(key);
    Collection<Window> assignedWindows = this.windowAssigner.assignWindow(window);
    for (Window w: assignedWindows) {
      if (!windows.containsKey(w)) {
        windows.put(w, 0);
      }
      windows.put(w, windows.get(w) + value);
    }
  }

  public Map<K, Integer> trigger(Long timestamp) {
    Map<K, Integer> results = new HashMap<K, Integer>();
    for (K key: windowInputs.keySet()) {
      TreeSortedMap<Window, Integer> windows = this.windowInputs.get(key);
      if (windows.notEmpty()) {
        Window firstKey = windows.firstKey();
        if (timestamp >= firstKey.getEnd()) {
          Integer inputs = windows.remove(firstKey);
          results.put(key, inputs);
        }
      }
    }
    return results;
  }
}
