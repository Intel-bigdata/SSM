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
import org.apache.hadoop.hdfs.server.namenode.metrics.FileAccessMetrics;
import org.smartdata.metrics.FileAccessEvent;
import org.smartdata.metrics.FileAccessEventCollector;
import org.smartdata.metrics.HDFSFileAccessEvent;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

public class NNMetricsAccessEventCollector implements FileAccessEventCollector {
  private static final List<FileAccessEvent> EMPTY_RESULT = new ArrayList<>();
  private FileAccessMetrics.Reader reader;
  private long now;

  @Override
  public void init(Configuration conf) {
    try {
      this.reader = FileAccessMetrics.Reader.create();
    } catch (IOException | URISyntaxException e) {
      e.printStackTrace();
    }
    now = System.currentTimeMillis();
  }

  @Override
  public List<FileAccessEvent> collect() throws IOException {
    try {
      if (reader.exists(now)) {
        reader.seekTo(now, false);

        List<FileAccessEvent> events = new ArrayList<>();
        while (reader.hasNext()) {
          FileAccessMetrics.Info info = reader.next();
          events.add(new HDFSFileAccessEvent(info.getPath(), info.getTimestamp()));
          now = info.getTimestamp();
        }
        return events;
      } else if (reader.exists(now + reader.getRollingIntervalMillis())) {
        // This is the corner case that AccessEventFetcher starts a little bit ahead of Namenode
        // and then Namenode begins log access event for the current rolling file, while
        // AccessCountFetch is seeking for the last one, which will never exist.
        now = now + reader.getRollingIntervalMillis() - now % reader.getRollingIntervalMillis();
      }
    } catch (IOException | URISyntaxException e) {
      e.printStackTrace();
    }
    return EMPTY_RESULT;
  }
}
