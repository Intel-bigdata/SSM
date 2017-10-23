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
package org.smartdata.metrics.impl;

import org.smartdata.metrics.FileAccessEvent;
import org.smartdata.metrics.FileAccessEventCollector;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Collect access events from users RPC call to Smart RPC Server.
 */
public class SmartServerAccessEventCollector implements FileAccessEventCollector {
  private final LinkedBlockingQueue<FileAccessEvent> outerQueue;

  public SmartServerAccessEventCollector(LinkedBlockingQueue<FileAccessEvent> queue) {
    this.outerQueue = queue;
  }

  @Override
  public List<FileAccessEvent> collect() throws IOException {
    FileAccessEvent[] events = new FileAccessEvent[outerQueue.size()];
    this.outerQueue.toArray(events);
    this.outerQueue.clear();
    return Arrays.asList(events);
  }
}
