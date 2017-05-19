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
package org.apache.hadoop.hdfs.server.namenode.metrics;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.sink.RollingFileSystemSink;

import java.io.PrintStream;
import java.util.LinkedList;
import java.util.List;


public class FileAccessMetrics implements MetricsSource {

  public static final String PREFIX = "FileAccess";
  public static final String CONTEXT_VALUE ="file_access";
  private List<FileAccessInfo> infos = new LinkedList<>();

  public static FileAccessMetrics create() {
    return create(DefaultMetricsSystem.initialize(PREFIX));
  }

  public static FileAccessMetrics create(MetricsSystem ms) {
    return ms.register("FileAccessMetrics", "FileAccessMetrics",
        new FileAccessMetrics());
  }

  public void add(String path, String user, long time) {
    infos.add(new FileAccessInfo(path, user, time));
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    for (FileAccessInfo info: infos) {
      MetricsRecordBuilder rb = collector.addRecord(info).setContext(CONTEXT_VALUE);
      rb.addGauge(info, 1);
    }
  }

  private static class FileAccessInfo implements MetricsInfo {
    private static final String SEPARATOR = ":";
    private String path;
    private String user;
    private long timestamp;

    FileAccessInfo(String path, String user, long timestamp) {
      this.path = path;
      this.user = user;
      this.timestamp = timestamp;
    }

    @Override
    public String name() {
      return path + SEPARATOR + user + SEPARATOR + timestamp;
    }

    @Override
    public String description() {
      return name();
    }
  }

  /**
   * Differs from its super class in that meta data is not written out for each record.
   */
  public static class FileAccessRollingFileSink extends RollingFileSystemSink {

    @Override
    public void putMetrics(PrintStream currentOutStream,
        FSDataOutputStream currentFSOutStream, MetricsRecord record) {
      for (AbstractMetric metric : record.metrics()) {
        currentOutStream.printf("%s=%s", metric.name(),
            metric.value());
      }
      currentOutStream.println();
    }
  }

}
