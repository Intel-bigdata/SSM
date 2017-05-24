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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSink;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.impl.ConfigBuilder;
import org.apache.hadoop.metrics2.impl.MetricsSystemImpl;
import org.apache.hadoop.metrics2.impl.TestMetricsConfig;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.metrics2.sink.RollingFileSystemSinkTestBase;
import org.apache.hadoop.util.Time;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestFileAccessMetrics extends RollingFileSystemSinkTestBase {

  private static final int  NUM_DATANODES = 4;
  private MiniDFSCluster cluster;

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

  /**
   * Test writing logs to HDFS.
   *
   * @throws Exception thrown when things break
   */
  @Test
  public void testWrite() throws Exception {
    String path = "hdfs://" + cluster.getNameNode().getHostAndPort() + "/tmp";
    MetricsSystem ms = initMetricsSystem(path, false, true);

    assertMetricsContents(doWriteTest(ms, path, 1));
  }

  @Override
  protected MetricsSystem initMetricsSystem(String path, boolean ignoreErrors,
      boolean allowAppend, boolean useSecureParams) {
    // If the prefix is not lower case, the metrics system won't be able to
    // read any of the properties.
    String prefix = methodName.getMethodName().toLowerCase();

    ConfigBuilder builder = new ConfigBuilder().add("*.period", 10000)
        .add(prefix + ".sink.mysink0.class", FileAccessMetrics.FileAccessRollingFileSink.class.getName())
        .add(prefix + ".sink.mysink0.basepath", path)
        .add(prefix + ".sink.mysink0.source", FileAccessMetrics.NAME)
        .add(prefix + ".sink.mysink0.context", FileAccessMetrics.CONTEXT_VALUE)
        .add(prefix + ".sink.mysink0.ignore-error", ignoreErrors)
        .add(prefix + ".sink.mysink0.allow-append", allowAppend)
        .add(prefix + ".sink.mysink0.roll-offset-interval-millis", 0)
        .add(prefix + ".sink.mysink0.roll-interval", "1h")
        .add(prefix + ".sink.mysink1.class", TestSink.class.getName())
        .add(prefix + ".sink.mysink1.source", "testsrc")
        .add(prefix + ".sink.mysink1.context", "test1");

    if (useSecureParams) {
      builder.add(prefix + ".sink.mysink0.keytab-key", SINK_KEYTAB_FILE_KEY)
          .add(prefix + ".sink.mysink0.principal-key", SINK_PRINCIPAL_KEY);
    }

    builder.save(TestMetricsConfig.getTestFilename("hadoop-metrics2-" + prefix));

    MetricsSystemImpl ms = new MetricsSystemImpl(prefix);

    ms.start();

    return ms;
  }

  @Override
  protected String doWriteTest(MetricsSystem ms, String path, int count)
      throws IOException, URISyntaxException {
    final String then = DATE_FORMAT.format(new Date()) + "00";

    FileAccessMetrics metrics = FileAccessMetrics.create(ms);
    metrics.add("path1", "user1", Time.now());
    metrics.add("path2", "", Time.now());

    TestMetrics tm = new TestMetrics().registerWith(ms);
    tm.testMetric1.incr();

    ms.publishMetricsNow(); // publish the metrics

    try {
      ms.stop();
    } finally {
      ms.shutdown();
    }

    return readLogFile(path, then, count);
  }

  @Override
  protected String readLogFile(String path, String then, int count)
      throws IOException, URISyntaxException {
    final String now = DATE_FORMAT.format(new Date()) + "00";
    final String logFile = FileAccessMetrics.NAME + "-" +
        InetAddress.getLocalHost().getHostName() + ".log";
    FileSystem fs = FileSystem.get(new URI(path), new Configuration());
    StringBuilder metrics = new StringBuilder();
    boolean found = false;

    for (FileStatus status : fs.listStatus(new Path(path))) {
      Path logDir = status.getPath();

      // There are only two possible valid log directory names: the time when
      // the test started and the current time.  Anything else can be ignored.
      if (now.equals(logDir.getName()) || then.equals(logDir.getName())) {
        readLogData(fs, findMostRecentLogFile(fs, new Path(logDir, logFile)),
            metrics);
        assertFileCount(fs, logDir, count);
        found = true;
      }
    }

    assertTrue("No valid log directories found", found);

    return metrics.toString();
  }

  @Override
  protected void assertExtraContents(String contents) {
    final Pattern expectedContentPattern = Pattern.compile(
        "Extra stuff[\\n\\r]*" +
        ".*:.*:\\d+=1$[\\n\\r]*" +
        ".*:.*:\\d+=1$[\\n\\r]*",
        Pattern.MULTILINE);

    assertTrue("Sink did not produce the expected output. Actual output was: "
        + contents, expectedContentPattern.matcher(contents).matches());
  }

  @Override
  protected void assertMetricsContents(String contents) {
    final Pattern expectedContentPattern = Pattern.compile(
        ".*:.*:\\d+=1$[\\n\\r]*" +
        ".*:.*:\\d+=1$[\\n\\r]*",
        Pattern.MULTILINE);

    assertTrue("Sink did not produce the expected output. Actual output was: "
        + contents, expectedContentPattern.matcher(contents).matches());
  }

  @Metrics(name="testRecord1", context="test1")
  private class TestMetrics {
    @Metric(value={"testMetric1", "An integer gauge"}, always=true)
    MutableGaugeInt testMetric1;

    public TestMetrics registerWith(MetricsSystem ms) {
      return ms.register(methodName.getMethodName() + "-m1", null, this);
    }
  }

  public static class TestSink implements MetricsSink {

    @Override
    public void init(SubsetConfiguration conf) {
    }

    @Override
    public void putMetrics(MetricsRecord record) {
      assertEquals("test1", record.context());
      assertEquals("testRecord1", record.name());
      AbstractMetric metric = record.metrics().iterator().next();
      assertEquals("testMetric1", metric.name());
      assertEquals(1, metric.value());
    }

    @Override
    public void flush() {
    }
  }

}
