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

import org.apache.hadoop.conf.Configuration;
import org.smartdata.metrics.FileAccessEventSource;

import java.io.IOException;

/**
 * A factory used to create FileAccessEventSource according to the configuration.
 */
public class MetricsFactory {
  private static final String ACCESS_EVENT_SOURCE = "smart.data.file.event.source";
  private static final String DEFAULT_ACCESS_EVENT_SOURCE =
    SmartServerAccessEventSource.class.getName();

  public static FileAccessEventSource createAccessEventSource(Configuration conf)
      throws IOException {
    String source = conf.get(ACCESS_EVENT_SOURCE, DEFAULT_ACCESS_EVENT_SOURCE);
    try {
      Class clazz = Class.forName(source);
      return (FileAccessEventSource) clazz.newInstance();
    } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
      e.printStackTrace();
      throw new IOException(e);
    }
  }
}
