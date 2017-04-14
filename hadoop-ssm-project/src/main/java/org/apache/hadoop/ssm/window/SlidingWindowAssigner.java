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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class SlidingWindowAssigner implements WindowAssigner {
  private final Long size;
  private final Long step;

  public SlidingWindowAssigner(Long size, Long step) {
    this.size = size;
    this.step = step;
  }

  @Override
  public Collection<Window> assignWindow(Window subWindow) {
    List<Window> results = new ArrayList<>();
    for (Window window: this.assignWindow(subWindow.getStart())) {
      if (window.include(subWindow)) {
        results.add(window);
      }
    }
    return results;
  }

  @Override
  public Collection<Window> assignWindow(Long timestamp) {
    List<Window> results = new ArrayList<Window>((int)(size / step));
    Long lastStartFor = lastStartFor(timestamp, step);
    for (long start = lastStartFor; start > timestamp - size; start -= step) {
      results.add(new Window(start, start + size));
    }
    return results;
  }

  private Long lastStartFor(Long timestamp, Long step) {
    return timestamp - (timestamp + step) % step;
  }
}
