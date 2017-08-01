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
package org.smartdata.hdfs.metric;

import org.smartdata.metrics.FileAccessEvent;
import org.smartdata.metrics.FileAccessEventCollector;
import org.smartdata.metrics.FileAccessEventSource;

public class NNMetricsAccessEventSource implements FileAccessEventSource {
  private final NNMetricsAccessEventCollector collector;

  public NNMetricsAccessEventSource() {
    this.collector = new NNMetricsAccessEventCollector();
  }

  @Override
  public FileAccessEventCollector getCollector() {
    return this.collector;
  }

  @Override
  public void insertEventFromSmartClient(FileAccessEvent event) {
    // Do nothing.
  }

  @Override
  public void close() {
    this.collector.close();
  }
}
