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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.commons.lang.time.FastDateFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.metrics2.MetricsException;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.metrics.FileAccessEvent;
import org.smartdata.metrics.FileAccessEventCollector;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NNMetricsAccessEventCollector implements FileAccessEventCollector {
  static final Logger LOG = LoggerFactory.getLogger(NNMetricsAccessEventCollector.class);

  private static final List<FileAccessEvent> EMPTY_RESULT = new ArrayList<>();
  private Reader reader;
  private long now;

  public NNMetricsAccessEventCollector() {
    try {
      this.reader = Reader.create();
    } catch (IOException | URISyntaxException e) {
      LOG.error("Create Reader error", e);
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
          Info info = reader.next();
          events.add(new FileAccessEvent(info.getPath(), info.getTimestamp()));
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
      LOG.error("FileAccessEvent collect error", e);
    }
    return EMPTY_RESULT;
  }

  public void close() {
    this.reader.close();
  }

  public static final FastDateFormat DATE_FORMAT =
    FastDateFormat.getInstance("yyyyMMddHHmm", TimeZone.getTimeZone("GMT"));
  private static final String BASEPATH_KEY = "basepath";
  private static final String BASEPATH_DEFAULT = "/tmp";
  private static final String SOURCE_KEY = "source";
  private static final String SOURCE_DEFAULT = "unknown";
  private static final String ROLL_INTERVAL_KEY = "roll-interval";
  private static final String DEFAULT_ROLL_INTERVAL = "1h";
  private static final String DEFAULT_FILE_NAME = "hadoop-metrics2.properties";
  private static final String EOF = "EOF";
  private static final String WATERMARK_VAL = "0";
  private static final String NORMAL_VAL = "1";
  private static final String INFO_SEPARATOR = ":";
  private static final String RECORD_SEPARATOR = "=";

  private static long getRollInterval(SubsetConfiguration properties) {
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
/*          case "s":
          case "sec":
          case "second":
          case "seconds":
            millis = TimeUnit.SECONDS.toMillis(rollIntervalInt);
            break;*/
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

  public static Path findMostRecentLogFile(FileSystem fs, Path initial)
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

  public static String getLogFileName(String source) throws UnknownHostException {
    return source + "-" + InetAddress.getLocalHost().getHostName() + ".log";
  }

  public static String getLogDirName(long time) {
    return DATE_FORMAT.format(time) + "00";
  }

  public static class Info implements MetricsInfo {
    private String path;
    private String user;
    private long timestamp;

    public String getPath() {
      return path;
    }

    public String getUser() {
      return user;
    }

    public long getTimestamp() {
      return timestamp;
    }

    public Info(String path, String user, long timestamp) {
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
      return path + INFO_SEPARATOR + user + INFO_SEPARATOR + timestamp;
    }

    @Override
    public String description() {
      return name();
    }
  }

  public static class Reader implements Iterator<Info>, Closeable {
    public static final Logger LOG = LoggerFactory.getLogger(Reader.class);
    private static final String PREFIX = "namenode.sink.file_access";
    private long rollIntervalMillis;
    private String basePath;
    private String source;
    private BufferedReader reader;
    private long curTime;
    private Info curInfo;
    private FileSystem fs;
    private boolean endOfFile;

    @VisibleForTesting
    Reader(SubsetConfiguration conf)
      throws URISyntaxException, IOException {
      basePath = conf.getString(BASEPATH_KEY, BASEPATH_DEFAULT);
      source = conf.getString(SOURCE_KEY, SOURCE_DEFAULT);
      rollIntervalMillis = getRollInterval(conf);
      fs = FileSystem.get(new URI(basePath), new Configuration());
    }

    public static Reader create()
      throws IOException, URISyntaxException {
      SubsetConfiguration properties =
        loadConfiguration(PREFIX,"hadoop-metrics2-" +
          PREFIX + ".properties", DEFAULT_FILE_NAME);
      return new Reader(properties);
    }

    @Override
    public boolean hasNext() {
      return curInfo != null;
    }

    @Override
    public Info next() {
      try {
        Info ret = curInfo;
        curInfo = readInfo();
        return ret;
      } catch (IOException | URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("remove");
    }

    @Override
    public void close() {
      try {
        if (reader != null) {
          reader.close();
        }
        fs.close();
      } catch (IOException e) {
        LOG.error(e.getMessage());
      }
    }

    private static SubsetConfiguration loadConfiguration(String prefix, String... fileNames) {
      for (String fname : fileNames) {
        try {
          org.apache.commons.configuration.Configuration cf = new PropertiesConfiguration(fname)
            .interpolatedConfiguration();
          LOG.info("loaded properties from " + fname);
          return new SubsetConfiguration(cf, prefix, ".");
        } catch (ConfigurationException e) {
          if (e.getMessage().startsWith("Cannot locate configuration")) {
            continue;
          }
          throw new RuntimeException(e);
        }
      }
      return new SubsetConfiguration(new PropertiesConfiguration(), prefix);
    }

    /**
     * seek to the first timestamp larger than @param timestamp
     *
     * @param timestamp target time
     * @param start whether to seek to file start
     * @return whether a valid timestamp is found
     * @throws IOException
     * @throws URISyntaxException
     */
    public boolean seekTo(long timestamp, boolean start)
      throws IOException, URISyntaxException {
      reader = getReader(timestamp);
      Info info;
      do {
        info = readInfo();
      } while (!start && info != null && info.timestamp <= timestamp);
      if (info != null) {
        curTime = timestamp;
      }
      curInfo = info;
      return curInfo != null;
    }

    /**
     * @param timestamp
     * @return whether the file containing the timestamp exists
     */
    public boolean exists(long timestamp) throws IOException {
      return fs.exists(getLogDir(basePath, timestamp, rollIntervalMillis));
    }

    public long getRollingIntervalMillis() {
      return rollIntervalMillis;
    }

    public boolean isEndOfFile() {
      return endOfFile;
    }

    private Info readInfo()
      throws IOException, URISyntaxException {
      if (reader != null) {
        String line = reader.readLine();
        if (line != null) {
          if (line.equals(EOF)) {
            endOfFile = true;
            reader.close();
            curTime += rollIntervalMillis;
            reader = getReader(curTime);
            return readInfo();
          } else {
            endOfFile = false;
            String[] kv = line.split(RECORD_SEPARATOR);
            if (kv.length == 2 && (kv[1].equals(WATERMARK_VAL) || kv[1].equals(NORMAL_VAL))) {
              String[] ks = kv[0].split(INFO_SEPARATOR);
              if (ks.length == 3) {
                return new Info(ks[0], ks[1], Long.parseLong(ks[2]));
              }
            }
          }
        }
      }
      return null;
    }

    private BufferedReader getReader(long time) throws URISyntaxException, IOException {
      Path dir = getLogDir(basePath, time, rollIntervalMillis);
      if (fs.exists(dir)) {
        Path file = findMostRecentLogFile(fs, new Path(dir, getLogFileName(source)));
        if (file != null) {
          return new BufferedReader(new InputStreamReader(fs.open(file),
            StandardCharsets.UTF_8));
        }
      }
      return null;
    }

    @VisibleForTesting
    protected static Path getLogDir(String base, long time, long interval) {
      String dir =
        getLogDirName(time / interval * interval);

      return new Path(base, dir);
    }
  }
}
