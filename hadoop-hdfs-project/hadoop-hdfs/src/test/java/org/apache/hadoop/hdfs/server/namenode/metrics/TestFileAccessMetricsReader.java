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

import org.apache.commons.configuration2.SubsetConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.metrics2.impl.ConfigBuilder;
import org.apache.hadoop.util.Time;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class TestFileAccessMetricsReader {

  private static final int  NUM_DATANODES = 4;
  private static final String SOURCE = "file_access";
  private static final long ROLL_INTERVAL_MILLIS = 10 * 60 * 1000; // 10m

  private MiniDFSCluster cluster;
  private String basePath;
  private int metricsPerFile;
  private int fileNum;

  public TestFileAccessMetricsReader(int metricsPerFile, int fileNum) {
    this.metricsPerFile = metricsPerFile;
    this.fileNum = fileNum;
  }

  /**
   * Create a {@link MiniDFSCluster} instance with four nodes.  The
   * node count is required to allow append to function. Also clear the
   * sink's test flags.
   *
   * @throws IOException thrown if cluster creation fails
   */
  @Before
  public void setupHdfs() throws IOException {
    Configuration conf = new Configuration();

    // It appears that since HDFS-265, append is always enabled.
    cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DATANODES).build();

    basePath = "hdfs://" + cluster.getNameNode().getHostAndPort() + "/path";
  }

  /**
   * Stop the {@link MiniDFSCluster}.
   */
  @After
  public void shutdownHdfs() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][]{
        { 1000, 1 },
        { 500, 2 },
        { 200, 5 },
        { 100, 10 },
        { 10, 100 }
    });
  }

  @Test
  public void testRead() throws IOException, URISyntaxException {
    long startTime = Time.now();
    int metricsNum = metricsPerFile * fileNum;
    List<FileAccessMetrics.Info> metrics = genMetrics(metricsNum, startTime);
    for (int i = 0; i < fileNum; i += 1) {
      doWrite(basePath, SOURCE, startTime + ROLL_INTERVAL_MILLIS * i,
          metrics.subList(i * metricsPerFile, (i + 1) * metricsPerFile));
    }

    verifyRead(metrics, startTime);
  }

  private void verifyRead(List<FileAccessMetrics.Info> metrics, long startTime)
      throws IOException, URISyntaxException {
    SubsetConfiguration conf = getConfiguration();
    FileAccessMetrics.Reader reader = new FileAccessMetrics.Reader(conf, startTime);
    for (FileAccessMetrics.Info expected: metrics) {
      assertTrue(reader.hasNext());
      assertEquals(expected, reader.next());
    }
    assertFalse(reader.hasNext());
  }

  private void doWrite(String basePath, String source, long time, List<FileAccessMetrics.Info> metrics)
      throws URISyntaxException, IOException {
    FileSystem fs = FileSystem.get(new URI(basePath), new Configuration());
    Path logDir = new Path(basePath, getLogDirName(time, 10 * 60 * 1000));
    fs.mkdirs(logDir);
    Path logFile = new Path(logDir, getLogFileName(source));
    FSDataOutputStream os = fs.create(logFile);
    PrintStream ps = new PrintStream(os, true, StandardCharsets.UTF_8.name());
    for (FileAccessMetrics.Info info: metrics) {
      ps.printf("%s=%s\n", info.name(), 1);
    }
    ps.close();
  }

  private List<FileAccessMetrics.Info> genMetrics(int num, long startTime) {
    List<FileAccessMetrics.Info> metrics = new ArrayList<>(num);
    for (int i = 0; i < num; i++) {
      metrics.add(new FileAccessMetrics.Info("path" + i, "user" + i, startTime + i));
    }
    return metrics;
  }

  private SubsetConfiguration getConfiguration() {
    ConfigBuilder builder = new ConfigBuilder();
    return builder.add("sink.roll-interval", "10m")
        .add("sink.basepath", basePath)
        .add("sink.source", SOURCE)
        .subset("sink");
  }

  private String getLogFileName(String src) throws UnknownHostException {
    return src + "-" + InetAddress.getLocalHost().getHostName() + ".log";
  }

  private String getLogDirName(long timestamp, long interval) {
    return FileAccessMetrics.DATE_FORMAT.format(
        timestamp / interval * interval) + "00";
  }
}
