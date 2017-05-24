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
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsException;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSink;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.TimeZone;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FileAccessMetrics implements MetricsSource {
  public static final String NAME = "FileAccessMetrics";
  public static final String DESC = "FileAccessMetrics";
  public static final String CONTEXT_VALUE ="file_access";
  public static final FastDateFormat DATE_FORMAT =
      FastDateFormat.getInstance("yyyyMMddHHmm", TimeZone.getTimeZone("GMT"));
  private static final String BASEPATH_KEY = "basepath";
  private static final String BASEPATH_DEFAULT = "/tmp";
  private static final String SOURCE_KEY = "source";
  private static final String SOURCE_DEFAULT = "unknown";
  private static final String ROLL_INTERVAL_KEY = "roll-interval";
  private static final String DEFAULT_ROLL_INTERVAL = "1h";
  private static final String DEFAULT_FILE_NAME = "hadoop-metrics2.properties";
  private static final String PREFIX = "NameNode";
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

  private static String getLogFileName(String source) throws UnknownHostException {
    return source + "-" + InetAddress.getLocalHost().getHostName() + ".log";
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
  public static class Writer implements MetricsSink, Closeable {

    private static final String IGNORE_ERROR_KEY = "ignore-error";
    private static final boolean DEFAULT_IGNORE_ERROR = false;
    private static final String ALLOW_APPEND_KEY = "allow-append";
    private static final boolean DEFAULT_ALLOW_APPEND = false;
    private static final String KEYTAB_PROPERTY_KEY = "keytab-key";
    private static final String USERNAME_PROPERTY_KEY = "principal-key";
    private static final String ROLL_OFFSET_INTERVAL_MILLIS_KEY =
        "roll-offset-interval-millis";
    private static final int DEFAULT_ROLL_OFFSET_INTERVAL_MILLIS = 30000;
    private final Object lock = new Object();
    private boolean initialized = false;
    private SubsetConfiguration properties;
    private Configuration conf;
    protected String source;
    private boolean ignoreError;
    private boolean allowAppend;
    private Path basePath;
    private FileSystem fileSystem;
    // The current directory path into which we're writing files
    private Path currentDirPath;
    // The path to the current file into which we're writing data
    private Path currentFilePath;
    // The stream to which we're currently writing.
    private PrintStream currentOutStream;
    // We keep this only to be able to call hsynch() on it.
    private FSDataOutputStream currentFSOutStream;
    private Timer flushTimer;
    // The amount of time between rolls
    private long rollIntervalMillis;
    // The maximum amount of random time to add to the initial roll
    private long rollOffsetIntervalMillis;
    // The time for the nextFlush
    private Calendar nextFlush = null;
    // This flag when true causes a metrics write to schedule a flush thread to
    // run immediately, but only if a flush thread is already scheduled. (It's a
    // timing thing.  If the first write forces the flush, it will strand the
    // second write.)
    private static boolean forceFlush = false;
    // This flag is used by the flusher thread to indicate that it has run. Used
    // only for testing purposes.
    private static volatile boolean hasFlushed = false;
    // Use this configuration instead of loading a new one.
    private static Configuration suppliedConf = null;
    // Use this file system instead of getting a new one.
    private static FileSystem suppliedFilesystem = null;

    /**
     * Create an empty instance.  Required for reflection.
     */
    public Writer() {
    }

    /**
     * Create an instance for testing.
     *
     * @param flushIntervalMillis the roll interval in millis
     * @param flushOffsetIntervalMillis the roll offset interval in millis
     */
    @VisibleForTesting
    protected Writer(long flushIntervalMillis,
        long flushOffsetIntervalMillis) {
      this.rollIntervalMillis = flushIntervalMillis;
      this.rollOffsetIntervalMillis = flushOffsetIntervalMillis;
    }

    @Override
    public void init(SubsetConfiguration metrics2Properties) {
      properties = metrics2Properties;
      basePath = new Path(properties.getString(BASEPATH_KEY, BASEPATH_DEFAULT));
      source = properties.getString(SOURCE_KEY, SOURCE_DEFAULT);
      ignoreError = properties.getBoolean(IGNORE_ERROR_KEY, DEFAULT_IGNORE_ERROR);
      allowAppend = properties.getBoolean(ALLOW_APPEND_KEY, DEFAULT_ALLOW_APPEND);
      rollOffsetIntervalMillis =
          getNonNegative(ROLL_OFFSET_INTERVAL_MILLIS_KEY,
              DEFAULT_ROLL_OFFSET_INTERVAL_MILLIS);
      rollIntervalMillis = getRollInterval(properties);

      conf = loadConf();
      UserGroupInformation.setConfiguration(conf);

      // Don't do secure setup if it's not needed.
      if (UserGroupInformation.isSecurityEnabled()) {
        // Validate config so that we don't get an NPE
        checkIfPropertyExists(KEYTAB_PROPERTY_KEY);
        checkIfPropertyExists(USERNAME_PROPERTY_KEY);


        try {
          // Login as whoever we're supposed to be and let the hostname be pulled
          // from localhost. If security isn't enabled, this does nothing.
          SecurityUtil.login(conf, properties.getString(KEYTAB_PROPERTY_KEY),
              properties.getString(USERNAME_PROPERTY_KEY));
        } catch (IOException ex) {
          throw new MetricsException("Error logging in securely: ["
              + ex.toString() + "]", ex);
        }
      }
    }

    /**
     * Initialize the connection to HDFS and create the base directory. Also
     * launch the flush thread.
     */
    private boolean initFs() {
      boolean success = false;

      fileSystem = getFileSystem();

      // This step isn't strictly necessary, but it makes debugging issues much
      // easier. We try to create the base directory eagerly and fail with
      // copious debug info if it fails.
      try {
        fileSystem.mkdirs(basePath);
        success = true;
      } catch (Exception ex) {
        if (!ignoreError) {
          throw new MetricsException("Failed to create " + basePath + "["
              + SOURCE_KEY + "=" + source + ", "
              + ALLOW_APPEND_KEY + "=" + allowAppend + ", "
              + stringifySecurityProperty(KEYTAB_PROPERTY_KEY) + ", "
              + stringifySecurityProperty(USERNAME_PROPERTY_KEY)
              + "] -- " + ex.toString(), ex);
        }
      }

      if (success) {
        // If we're permitted to append, check if we actually can
        if (allowAppend) {
          allowAppend = checkAppend(fileSystem);
        }

        flushTimer = new Timer("RollingFileSystemSink Flusher", true);
        setInitialFlushTime(new Date());
      }

      return success;
    }

    /**
     * Turn a security property into a nicely formatted set of <i>name=value</i>
     * strings, allowing for either the property or the configuration not to be
     * set.
     *
     * @param property the property to stringify
     * @return the stringified property
     */
    private String stringifySecurityProperty(String property) {
      String securityProperty;

      if (properties.containsKey(property)) {
        String propertyValue = properties.getString(property);
        String confValue = conf.get(properties.getString(property));

        if (confValue != null) {
          securityProperty = property + "=" + propertyValue
              + ", " + properties.getString(property) + "=" + confValue;
        } else {
          securityProperty = property + "=" + propertyValue
              + ", " + properties.getString(property) + "=<NOT SET>";
        }
      } else {
        securityProperty = property + "=<NOT SET>";
      }

      return securityProperty;
    }

    /**
     * Return the property value if it's non-negative and throw an exception if
     * it's not.
     *
     * @param key the property key
     * @param defaultValue the default value
     */
    private long getNonNegative(String key, int defaultValue) {
      int flushOffsetIntervalMillis = properties.getInt(key, defaultValue);

      if (flushOffsetIntervalMillis < 0) {
        throw new MetricsException("The " + key + " property must be "
            + "non-negative. Value was " + flushOffsetIntervalMillis);
      }

      return flushOffsetIntervalMillis;
    }

    /**
     * Throw a {@link MetricsException} if the given property is not set.
     *
     * @param key the key to validate
     */
    private void checkIfPropertyExists(String key) {
      if (!properties.containsKey(key)) {
        throw new MetricsException("Metrics2 configuration is missing " + key
            + " property");
      }
    }

    /**
     * Return the supplied configuration for testing or otherwise load a new
     * configuration.
     *
     * @return the configuration to use
     */
    private Configuration loadConf() {
      Configuration c;

      if (suppliedConf != null) {
        c = suppliedConf;
      } else {
        // The config we're handed in init() isn't the one we want here, so we
        // create a new one to pick up the full settings.
        c = new Configuration();
      }

      return c;
    }

    /**
     * Return the supplied file system for testing or otherwise get a new file
     * system.
     *
     * @return the file system to use
     * @throws MetricsException thrown if the file system could not be retrieved
     */
    private FileSystem getFileSystem() throws MetricsException {
      FileSystem fs = null;

      if (suppliedFilesystem != null) {
        fs = suppliedFilesystem;
      } else {
        try {
          fs = FileSystem.get(new URI(basePath.toString()), conf);
        } catch (URISyntaxException ex) {
          throw new MetricsException("The supplied filesystem base path URI"
              + " is not a valid URI: " + basePath.toString(), ex);
        } catch (IOException ex) {
          throw new MetricsException("Error connecting to file system: "
              + basePath + " [" + ex.toString() + "]", ex);
        }
      }

      return fs;
    }

    /**
     * Test whether the file system supports append and return the answer.
     *
     * @param fs the target file system
     */
    private boolean checkAppend(FileSystem fs) {
      boolean canAppend = true;

      try {
        fs.append(basePath);
      } catch (UnsupportedOperationException ex) {
        canAppend = false;
      } catch (IOException ex) {
        // Ignore. The operation is supported.
      }

      return canAppend;
    }

    /**
     * Check the current directory against the time stamp.  If they're not
     * the same, create a new directory and a new log file in that directory.
     *
     * @throws MetricsException thrown if an error occurs while creating the
     * new directory or new log file
     */
    private void rollLogDirIfNeeded() throws MetricsException {
      // Because we're working relative to the clock, we use a Date instead
      // of Time.monotonicNow().
      Date now = new Date();

      // We check whether currentOutStream is null instead of currentDirPath,
      // because if currentDirPath is null, then currentOutStream is null, but
      // currentOutStream can be null for other reasons.  Same for nextFlush.
      if ((currentOutStream == null) || now.after(nextFlush.getTime())) {
        // If we're not yet connected to HDFS, create the connection
        if (!initialized) {
          initialized = initFs();
        }

        if (initialized) {
          // Close the stream. This step could have been handled already by the
          // flusher thread, but if it has, the PrintStream will just swallow the
          // exception, which is fine.
          if (currentOutStream != null) {
            currentOutStream.close();
          }

          currentDirPath = findCurrentDirectory(now);

          try {
            rollLogDir();
          } catch (IOException ex) {
            throwMetricsException("Failed to create new log file", ex);
          }

          // Update the time of the next flush
          updateFlushTime(now);
          // Schedule the next flush at that time
          scheduleFlush(nextFlush.getTime());
        }
      } else if (forceFlush) {
        scheduleFlush(new Date());
      }
    }

    /**
     * Use the given time to determine the current directory. The current
     * directory will be based on the {@link #rollIntervalMillis}.
     *
     * @param now the current time
     * @return the current directory
     */
    private Path findCurrentDirectory(Date now) {
      long offset = ((now.getTime() - nextFlush.getTimeInMillis())
          / rollIntervalMillis) * rollIntervalMillis;
      String currentDir =
          DATE_FORMAT.format(new Date(nextFlush.getTimeInMillis() + offset));

      return new Path(basePath, currentDir);
    }

    /**
     * Schedule the current interval's directory to be flushed. If this ends up
     * running after the top of the next interval, it will execute immediately.
     *
     * @param when the time the thread should run
     */
    private void scheduleFlush(Date when) {
      // Store the current currentDirPath to close later
      final PrintStream toClose = currentOutStream;

      flushTimer.schedule(new TimerTask() {
        @Override
        public void run() {
          synchronized (lock) {
            // This close may have already been done by a putMetrics() call. If it
            // has, the PrintStream will swallow the exception, which is fine.
            toClose.close();
          }

          hasFlushed = true;
        }
      }, when);
    }

    /**
     * Update the {@link #nextFlush} variable to the next flush time. Add
     * an integer number of flush intervals, preserving the initial random offset.
     *
     * @param now the current time
     */
    @VisibleForTesting
    protected void updateFlushTime(Date now) {
      // In non-initial rounds, add an integer number of intervals to the last
      // flush until a time in the future is achieved, thus preserving the
      // original random offset.
      int millis =
          (int) (((now.getTime() - nextFlush.getTimeInMillis())
              / rollIntervalMillis + 1) * rollIntervalMillis);

      nextFlush.add(Calendar.MILLISECOND, millis);
    }

    /**
     * Set the {@link #nextFlush} variable to the initial flush time. The initial
     * flush will be an integer number of flush intervals past the beginning of
     * the current hour and will have a random offset added, up to
     * {@link #rollOffsetIntervalMillis}. The initial flush will be a time in
     * past that can be used from which to calculate future flush times.
     *
     * @param now the current time
     */
    @VisibleForTesting
    protected void setInitialFlushTime(Date now) {
      // Start with the beginning of the current hour
      nextFlush = Calendar.getInstance();
      nextFlush.setTime(now);
      nextFlush.set(Calendar.MILLISECOND, 0);
      nextFlush.set(Calendar.SECOND, 0);
      nextFlush.set(Calendar.MINUTE, 0);

      // In the first round, calculate the first flush as the largest number of
      // intervals from the beginning of the current hour that's not in the
      // future by:
      // 1. Subtract the beginning of the hour from the current time
      // 2. Divide by the roll interval and round down to get the number of whole
      //    intervals that have passed since the beginning of the hour
      // 3. Multiply by the roll interval to get the number of millis between
      //    the beginning of the current hour and the beginning of the current
      //    interval.
      int millis = (int) (((now.getTime() - nextFlush.getTimeInMillis())
          / rollIntervalMillis) * rollIntervalMillis);

      // Then add some noise to help prevent all the nodes from
      // closing their files at the same time.
      if (rollOffsetIntervalMillis > 0) {
        millis += ThreadLocalRandom.current().nextLong(rollOffsetIntervalMillis);

        // If the added time puts us into the future, step back one roll interval
        // because the code to increment nextFlush to the next flush expects that
        // nextFlush is the next flush from the previous interval.  There wasn't
        // a previous interval, so we just fake it with the time in the past that
        // would have been the previous interval if there had been one.
        //
        // It's OK if millis comes out negative.
        while (nextFlush.getTimeInMillis() + millis > now.getTime()) {
          millis -= rollIntervalMillis;
        }
      }

      // Adjust the next flush time by millis to get the time of our ficticious
      // previous next flush
      nextFlush.add(Calendar.MILLISECOND, millis);
    }

    /**
     * Create a new directory based on the current interval and a new log file in
     * that directory.
     *
     * @throws IOException thrown if an error occurs while creating the
     * new directory or new log file
     */
    private void rollLogDir() throws IOException {
      String fileName = getLogFileName(source);

      Path targetFile = new Path(currentDirPath, fileName);
      fileSystem.mkdirs(currentDirPath);

      if (allowAppend) {
        createOrAppendLogFile(targetFile);
      } else {
        createLogFile(targetFile);
      }
    }

    /**
     * Create a new log file and return the {@link FSDataOutputStream}. If a
     * file with the specified path already exists, add a suffix, starting with 1
     * and try again. Keep incrementing the suffix until a nonexistent target
     * path is found.
     *
     * Once the file is open, update {@link #currentFSOutStream},
     * {@link #currentOutStream}, and {@#link #currentFilePath} are set
     * appropriately.
     *
     * @param initial the target path
     * @throws IOException thrown if the call to see if the exists fails
     */
    private void createLogFile(Path initial) throws IOException {
      Path currentAttempt = initial;
      // Start at 0 so that if the base filname exists, we start with the suffix
      // ".1".
      int id = 0;

      while (true) {
        // First try blindly creating the file. If we fail, it either means
        // the file exists, or the operation actually failed.  We do it this way
        // because if we check whether the file exists, it might still be created
        // by the time we try to create it. Creating first works like a
        // test-and-set.
        try {
          currentFSOutStream = fileSystem.create(currentAttempt, false);
          currentOutStream = new PrintStream(currentFSOutStream, true,
              StandardCharsets.UTF_8.name());
          currentFilePath = currentAttempt;
          break;
        } catch (IOException ex) {
          // Now we can check to see if the file exists to know why we failed
          if (fileSystem.exists(currentAttempt)) {
            id = getNextIdToTry(initial, id);
            currentAttempt = new Path(initial.toString() + "." + id);
          } else {
            throw ex;
          }
        }
      }
    }

    /**
     * Return the next ID suffix to use when creating the log file. This method
     * will look at the files in the directory, find the one with the highest
     * ID suffix, and 1 to that suffix, and return it. This approach saves a full
     * linear probe, which matters in the case where there are a large number of
     * log files.
     *
     * @param initial the base file path
     * @param lastId the last ID value that was used
     * @return the next ID to try
     * @throws IOException thrown if there's an issue querying the files in the
     * directory
     */
    private int getNextIdToTry(Path initial, int lastId)
        throws IOException {
      RemoteIterator<LocatedFileStatus> files =
          fileSystem.listFiles(currentDirPath, true);
      String base = initial.toString();
      int id = lastId;

      while (files.hasNext()) {
        String file = files.next().getPath().getName();

        if (file.startsWith(base)) {
          int fileId = extractId(file);

          if (fileId > id) {
            id = fileId;
          }
        }
      }

      // Return either 1 more than the highest we found or 1 more than the last
      // ID used (if no ID was found).
      return id + 1;
    }

    /**
     * Extract the ID from the suffix of the given file name.
     *
     * @param file the file name
     * @return the ID or -1 if no ID could be extracted
     */
    private int extractId(String file) {
      int index = file.lastIndexOf(".");
      int id = -1;

      // A hostname has to have at least 1 character
      if (index > 0) {
        try {
          id = Integer.parseInt(file.substring(index + 1));
        } catch (NumberFormatException ex) {
          // This can happen if there's no suffix, but there is a dot in the
          // hostname.  Just ignore it.
        }
      }

      return id;
    }

    /**
     * Create a new log file and return the {@link FSDataOutputStream}. If a
     * file with the specified path already exists, open the file for append
     * instead.
     *
     * Once the file is open, update {@link #currentFSOutStream},
     * {@link #currentOutStream}, and {@#link #currentFilePath}.
     *
     * @param targetFile the target path
     * @throws IOException thrown if the call to see the append operation fails.
     */
    private void createOrAppendLogFile(Path targetFile) throws IOException {
      // First try blindly creating the file. If we fail, it either means
      // the file exists, or the operation actually failed.  We do it this way
      // because if we check whether the file exists, it might still be created
      // by the time we try to create it. Creating first works like a
      // test-and-set.
      try {
        currentFSOutStream = fileSystem.create(targetFile, false);
        currentOutStream = new PrintStream(currentFSOutStream, true,
            StandardCharsets.UTF_8.name());
      } catch (IOException ex) {
        // Try appending instead.  If we fail, if means the file doesn't
        // actually exist yet or the operation actually failed.
        try {
          currentFSOutStream = fileSystem.append(targetFile);
          currentOutStream = new PrintStream(currentFSOutStream, true,
              StandardCharsets.UTF_8.name());
        } catch (IOException ex2) {
          // If the original create failed for a legit but transitory
          // reason, the append will fail because the file now doesn't exist,
          // resulting in a confusing stack trace.  To avoid that, we set
          // the cause of the second exception to be the first exception.
          // It's still a tiny bit confusing, but it's enough
          // information that someone should be able to figure it out.
          ex2.initCause(ex);

          throw ex2;
        }
      }

      currentFilePath = targetFile;
    }

    @Override
    public void putMetrics(MetricsRecord record) {
      synchronized (lock) {
        rollLogDirIfNeeded();

        if (currentOutStream != null) {
          for (AbstractMetric metric : record.metrics()) {
            currentOutStream.printf("%s=%s", metric.name(),
                metric.value());
          }
          currentOutStream.println();

          checkForErrors("Unable to write to log file");
        } else if (!ignoreError) {
          throwMetricsException("Unable to write to log file");
        }
      }
    }

    @Override
    public void flush() {
      synchronized (lock) {
        // currentOutStream is null if currentFSOutStream is null
        if (currentFSOutStream != null) {
          try {
            currentFSOutStream.hflush();
          } catch (IOException ex) {
            throwMetricsException("Unable to flush log file", ex);
          }
        }
      }
    }

    @Override
    public void close() {
      synchronized (lock) {
        if (currentOutStream != null) {
          currentOutStream.close();

          try {
            checkForErrors("Unable to close log file");
          } finally {
            // Null out the streams just in case someone tries to reuse us.
            currentOutStream = null;
            currentFSOutStream = null;
          }
        }
      }
    }

    /**
     * If the sink isn't set to ignore errors, throw a {@link MetricsException}
     * if the stream encountered an exception.  The message parameter will be used
     * as the new exception's message with the current file name
     * ({@link #currentFilePath}) appended to it.
     *
     * @param message the exception message. The message will have a colon and
     * the current file name ({@link #currentFilePath}) appended to it.
     * @throws MetricsException thrown if there was an error and the sink isn't
     * ignoring errors
     */
    private void checkForErrors(String message)
        throws MetricsException {
      if (!ignoreError && currentOutStream.checkError()) {
        throw new MetricsException(message + ": " + currentFilePath);
      }
    }

    /**
     * If the sink isn't set to ignore errors, wrap the Throwable in a
     * {@link MetricsException} and throw it.  The message parameter will be used
     * as the new exception's message with the current file name
     * ({@link #currentFilePath}) and the Throwable's string representation
     * appended to it.
     *
     * @param message the exception message. The message will have a colon, the
     * current file name ({@link #currentFilePath}), and the Throwable's string
     * representation (wrapped in square brackets) appended to it.
     * @param t the Throwable to wrap
     */
    private void throwMetricsException(String message, Throwable t) {
      if (!ignoreError) {
        throw new MetricsException(message + ": " + currentFilePath + " ["
            + t.toString() + "]", t);
      }
    }

    /**
     * If the sink isn't set to ignore errors, throw a new
     * {@link MetricsException}.  The message parameter will be used  as the
     * new exception's message with the current file name
     * ({@link #currentFilePath}) appended to it.
     *
     * @param message the exception message. The message will have a colon and
     * the current file name ({@link #currentFilePath}) appended to it.
     */
    private void throwMetricsException(String message) {
      if (!ignoreError) {
        throw new MetricsException(message + ": " + currentFilePath);
      }
    }
  }

  public static class Reader implements Iterator<Info> {

    public static final Logger LOG = LoggerFactory.getLogger(Reader.class);

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

    private BufferedReader getReader(long time) throws URISyntaxException, IOException {
      Path dir = getLogDir(time);
      if (fs.exists(dir)) {
        Path file = findMostRecentLogFile(new Path(dir, getLogFileName(source)));
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
  }

}
