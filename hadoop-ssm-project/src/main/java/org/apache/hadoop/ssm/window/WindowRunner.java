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

import com.gs.collections.impl.list.mutable.FastList;
import com.gs.collections.impl.map.mutable.UnifiedMap;
import com.gs.collections.impl.map.sorted.mutable.TreeSortedMap;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class WindowRunner<IN, OUT> {
  private UnifiedMap<IN, TreeSortedMap<Window, FastList<OUT>>> windowInputs;
  private WindowAssigner windowAssigner;

  public WindowRunner(WindowAssigner windowAssigner) {
    this.windowAssigner = windowAssigner;
    this.windowInputs = new UnifiedMap<>();
  }

  public void process(IN key, Window window, OUT value) {
    if (!this.windowInputs.containsKey(key)) {
      this.windowInputs.put(key, new TreeSortedMap<>());
    }
    TreeSortedMap<Window, FastList<OUT>> windows = this.windowInputs.get(key);
    Collection<Window> assignedWindows = this.windowAssigner.assignWindow(window);
    for (Window w: assignedWindows) {
      if (!windows.containsKey(w)) {
        FastList<OUT> values = new FastList<>(1);
        windows.put(w, values);
      }
      windows.get(w).add(value);
    }
  }

  public Map<IN, Collection<OUT>> trigger(Long timestamp) {
    Map<IN, Collection<OUT>> results = new HashMap<IN, Collection<OUT>>();
    for (IN key: windowInputs.keySet()) {
      TreeSortedMap<Window, FastList<OUT>> windows = this.windowInputs.get(key);
      if (windows.notEmpty()) {
        Window firstKey = windows.firstKey();
        if (timestamp >= firstKey.getEnd()) {
          FastList<OUT> inputs = windows.remove(firstKey);
          results.put(key, inputs);
        }
      }
    }
    return results;
  }
}
