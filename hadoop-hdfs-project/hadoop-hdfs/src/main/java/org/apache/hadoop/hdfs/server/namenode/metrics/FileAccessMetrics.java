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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.SubsetConfiguration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.lang.time.FastDateFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsException;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.sink.RollingFileSystemSink;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class FileAccessMetrics implements MetricsSource {
  public static final String NAME = "FileAccessMetrics";
  public static final String DESC = "FileAccessMetrics";
  public static final String CONTEXT_VALUE ="file_access";
  private List<Info> infos = new LinkedList<>();

  public static FileAccessMetrics create(MetricsSystem ms) {
    return ms.register(NAME, DESC, new FileAccessMetrics());
  }

  public void add(String path, String user, long time) {
    infos.add(new Info(path, user, time));
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    for (Info info: infos) {
      MetricsRecordBuilder rb = collector.addRecord(info).setContext(CONTEXT_VALUE);
      rb.addGauge(info, 1);
    }
  }

  public static class Info implements MetricsInfo {
    private static final String SEPARATOR = ":";
    private String path;
    private String user;
    private long timestamp;

    Info(String path, String user, long timestamp) {
      this.path = path;
      this.user = user;
      this.timestamp = timestamp;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Info that = (Info) o;

      if (timestamp != that.timestamp) return false;
      if (path != null ? !path.equals(that.path) : that.path != null) return false;
      return user != null ? user.equals(that.user) : that.user == null;
    }

    @Override
    public int hashCode() {
      int result = path != null ? path.hashCode() : 0;
      result = 31 * result + (user != null ? user.hashCode() : 0);
      result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
      return result;
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
  public static class Writer extends RollingFileSystemSink {

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

  public static class Reader implements Iterator<Info> {

    public static final Logger LOG = LoggerFactory.getLogger(Reader.class);
    static final FastDateFormat DATE_FORMAT =
        FastDateFormat.getInstance("yyyyMMddHHmm", TimeZone.getTimeZone("GMT"));
    private static final String BASEPATH_KEY = "basepath";
    private static final String BASEPATH_DEFAULT = "/tmp";
    private static final String SOURCE_KEY = "source";
    private static final String SOURCE_DEFAULT = "unknown";
    private static final String ROLL_INTERVAL_KEY = "roll-interval";
    private static final String DEFAULT_ROLL_INTERVAL = "1h";
    private static final String DEFAULT_FILE_NAME = "hadoop-metrics2.properties";
    private static final String PREFIX = "NameNode";

    private long rollIntervalMillis;
    private String basePath;
    private String source;
    private BufferedReader reader;
    private long curTime;
    private Info curInfo;
    private FileSystem fs;

    @VisibleForTesting
    Reader(SubsetConfiguration conf, long startTime)
        throws URISyntaxException, IOException {
      basePath = conf.getString(BASEPATH_KEY, BASEPATH_DEFAULT);
      source = conf.getString(SOURCE_KEY, SOURCE_DEFAULT);
      rollIntervalMillis = getRollInterval(conf);
      fs = FileSystem.get(new URI(basePath), new Configuration());
      curTime = startTime;
      seekTo(curTime);
    }

    public static Reader create(long startTime)
        throws IOException, URISyntaxException {
      SubsetConfiguration properties =
          loadConfiguration(PREFIX,"hadoop-metrics2-" +
              StringUtils.toLowerCase(PREFIX) + ".properties", DEFAULT_FILE_NAME);
      return new Reader(properties, startTime);
    }

    @Override
    public boolean hasNext() {
      return curInfo != null;
    }

    @Override
    public Info next() {
      try {
        Info ret = curInfo;
        Info nextInfo = readInfo(reader);
        if (null == nextInfo) {
          // try reading next file
          curTime += rollIntervalMillis;
          reader = getReader(curTime);
          nextInfo = readInfo(reader);
        }
        curInfo = nextInfo;
        return ret;
      } catch (IOException | URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }

    public void close() {
      try {
        fs.close();
      } catch (IOException e) {
        LOG.error(e.getMessage());
      }
    }

    private static SubsetConfiguration loadConfiguration(String prefix, String... fileNames) {
      for (String fname : fileNames) {
        try {
          org.apache.commons.configuration2.Configuration cf =
              new Configurations().propertiesBuilder(fname)
                  .configure(new Parameters().properties()
                      .setFileName(fname)
                      .setListDelimiterHandler(new DefaultListDelimiterHandler(',')))
                  .getConfiguration()
                  .interpolatedConfiguration();
          LOG.info("loaded properties from " + fname);
          return new SubsetConfiguration(cf, prefix, ".");
        } catch (ConfigurationException e) {
          throw new RuntimeException(e);
        }
      }
      return new SubsetConfiguration(new PropertiesConfiguration(), prefix);
    }

    private void seekTo(long target)
        throws IOException, URISyntaxException {
      reader = getReader(target);
      Info info;
      do {
        info = readInfo(reader);
      } while (info != null && info.timestamp > target);
      curInfo = info;
    }

    private Info readInfo(BufferedReader reader) throws IOException {
      if (reader != null) {
        String line = reader.readLine();
        if (line != null) {
          String[] parts = line.split("=")[0].split(":");
          return new Info(parts[0], parts[1],
              Long.parseLong(parts[2]));
        }
      }
      return null;
    }

    private Path findMostRecentLogFile(Path initial)
        throws IOException {
      Path logFile = null;
      Path nextLogFile = initial;
      int id = 1;

      do {
        logFile = nextLogFile;
        nextLogFile = new Path(initial.toString() + "." + id);
        id += 1;
      } while (fs.exists(nextLogFile));

      return logFile;
    }

    private String getLogFilename(String source) throws UnknownHostException {
      return source + "-" + InetAddress.getLocalHost().getHostName() + ".log";
    }

    private BufferedReader getReader(long time) throws URISyntaxException, IOException {
      Path dir = getLogDir(time);
      if (fs.exists(dir)) {
        Path file = findMostRecentLogFile(new Path(dir, getLogFilename(source)));
        if (file != null) {
          return new BufferedReader(new InputStreamReader(fs.open(file),
              StandardCharsets.UTF_8));
        }
      }
      return null;
    }

    private Path getLogDir(long time) {
      String dir =
          DATE_FORMAT.format(new Date(time / rollIntervalMillis * rollIntervalMillis)) + "00";

      return new Path(basePath, dir);
    }


    private long getRollInterval(SubsetConfiguration properties) {
      String rollInterval =
          properties.getString(ROLL_INTERVAL_KEY, DEFAULT_ROLL_INTERVAL);
      Pattern pattern = Pattern.compile("^\\s*(\\d+)\\s*([A-Za-z]*)\\s*$");
      Matcher match = pattern.matcher(rollInterval);
      long millis;

      if (match.matches()) {
        String flushUnit = match.group(2);
        int rollIntervalInt;

        try {
          rollIntervalInt = Integer.parseInt(match.group(1));
        } catch (NumberFormatException ex) {
          throw new MetricsException("Unrecognized flush interval: "
              + rollInterval + ". Must be a number followed by an optional "
              + "unit. The unit must be one of: minute, hour, day", ex);
        }

        if ("".equals(flushUnit)) {
          millis = TimeUnit.HOURS.toMillis(rollIntervalInt);
        } else {
          switch (flushUnit.toLowerCase()) {
            case "m":
            case "min":
            case "minute":
            case "minutes":
              millis = TimeUnit.MINUTES.toMillis(rollIntervalInt);
              break;
            case "h":
            case "hr":
            case "hour":
            case "hours":
              millis = TimeUnit.HOURS.toMillis(rollIntervalInt);
              break;
            case "d":
            case "day":
            case "days":
              millis = TimeUnit.DAYS.toMillis(rollIntervalInt);
              break;
            default:
              throw new MetricsException("Unrecognized unit for flush interval: "
                  + flushUnit + ". Must be one of: minute, hour, day");
          }
        }
      } else {
        throw new MetricsException("Unrecognized flush interval: "
            + rollInterval + ". Must be a number followed by an optional unit."
            + " The unit must be one of: minute, hour, day");
      }

      if (millis < 60000) {
        throw new MetricsException("The flush interval property must be "
            + "at least 1 minute. Value was " + rollInterval);
      }

      return millis;
    }


  }

}
