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
package org.smartdata.erasurecode.test;

import com.google.common.base.Joiner;
import com.google.common.base.Supplier;
import com.google.common.collect.Sets;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.log4j.*;
import org.junit.Assert;
import org.junit.Assume;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

/**
 * Test provides some very generic helpers which might be used across the tests
 */
public abstract class GenericTestUtils {

  private static final AtomicInteger sequence = new AtomicInteger();

  /**
   * system property for test data: {@value}
   */
  public static final String SYSPROP_TEST_DATA_DIR = "test.build.data";

  /**
   * Default path for test data: {@value}
   */
  public static final String DEFAULT_TEST_DATA_DIR =
      "target" + File.separator + "org/apache/hadoop/test" + File.separator + "data";

  /**
   * The default path for using in Hadoop path references: {@value}
   */
  public static final String DEFAULT_TEST_DATA_PATH = "target/test/data/";

  @SuppressWarnings("unchecked")
  public static void disableLog(Log log) {
    // We expect that commons-logging is a wrapper around Log4j.
    disableLog((Log4JLogger) log);
  }

  public static Logger toLog4j(org.slf4j.Logger logger) {
    return LogManager.getLogger(logger.getName());
  }

  public static void disableLog(Log4JLogger log) {
    log.getLogger().setLevel(Level.OFF);
  }

  public static void disableLog(Logger logger) {
    logger.setLevel(Level.OFF);
  }

  public static void disableLog(org.slf4j.Logger logger) {
    disableLog(toLog4j(logger));
  }

  @SuppressWarnings("unchecked")
  public static void setLogLevel(Log log, Level level) {
    // We expect that commons-logging is a wrapper around Log4j.
    setLogLevel((Log4JLogger) log, level);
  }

  public static void setLogLevel(Log4JLogger log, Level level) {
    log.getLogger().setLevel(level);
  }

  public static void setLogLevel(Logger logger, Level level) {
    logger.setLevel(level);
  }

  public static void setLogLevel(org.slf4j.Logger logger, Level level) {
    setLogLevel(toLog4j(logger), level);
  }

  /**
   * Extracts the name of the method where the invocation has happened
   * @return String name of the invoking method
   */
  public static String getMethodName() {
    return Thread.currentThread().getStackTrace()[2].getMethodName();
  }

  /**
   * Generates a process-wide unique sequence number.
   * @return an unique sequence number
   */
  public static int uniqueSequenceId() {
    return sequence.incrementAndGet();
  }

  /**
   * Get the (created) base directory for tests.
   * @return the absolute directory
   */
  public static File getTestDir() {
    String prop = System.getProperty(SYSPROP_TEST_DATA_DIR, DEFAULT_TEST_DATA_DIR);
    if (prop.isEmpty()) {
      // corner case: property is there but empty
      prop = DEFAULT_TEST_DATA_DIR;
    }
    File dir = new File(prop).getAbsoluteFile();
    dir.mkdirs();
    assertExists(dir);
    return dir;
  }

  /**
   * Get an uncreated directory for tests.
   * @return the absolute directory for tests. Caller is expected to create it.
   */
  public static File getTestDir(String subdir) {
    return new File(getTestDir(), subdir).getAbsoluteFile();
  }

  /**
   * Get an uncreated directory for tests with a randomized alphanumeric
   * name. This is likely to provide a unique path for tests run in parallel
   * @return the absolute directory for tests. Caller is expected to create it.
   */
  public static File getRandomizedTestDir() {
    return new File(getRandomizedTempPath()).getAbsoluteFile();
  }

  /**
   * Get a temp path. This may or may not be relative; it depends on what the
   * {@link #SYSPROP_TEST_DATA_DIR} is set to. If unset, it returns a path
   * under the relative path {@link #DEFAULT_TEST_DATA_PATH}
   * @param subpath sub path, with no leading "/" character
   * @return a string to use in paths
   */
  public static String getTempPath(String subpath) {
    String prop = System.getProperty(SYSPROP_TEST_DATA_DIR, DEFAULT_TEST_DATA_PATH);
    if (prop.isEmpty()) {
      // corner case: property is there but empty
      prop = DEFAULT_TEST_DATA_PATH;
    }
    if (!prop.endsWith("/")) {
      prop = prop + "/";
    }
    return prop + subpath;
  }

  /**
   * Get a temp path. This may or may not be relative; it depends on what the
   * {@link #SYSPROP_TEST_DATA_DIR} is set to. If unset, it returns a path
   * under the relative path {@link #DEFAULT_TEST_DATA_PATH}
   * @param subpath sub path, with no leading "/" character
   * @return a string to use in paths
   */
  public static String getRandomizedTempPath() {
    return getTempPath(RandomStringUtils.randomAlphanumeric(10));
  }

  /**
   * Assert that a given file exists.
   */
  public static void assertExists(File f) {
    Assert.assertTrue("File " + f + " should exist", f.exists());
  }
    
  /**
   * List all of the files in 'dir' that match the regex 'pattern'.
   * Then check that this list is identical to 'expectedMatches'.
   * @throws IOException if the dir is inaccessible
   */
  public static void assertGlobEquals(File dir, String pattern,
      String ... expectedMatches) throws IOException {
    
    Set<String> found = Sets.newTreeSet();
    for (File f : FileUtil.listFiles(dir)) {
      if (f.getName().matches(pattern)) {
        found.add(f.getName());
      }
    }
    Set<String> expectedSet = Sets.newTreeSet(
        Arrays.asList(expectedMatches));
    Assert.assertEquals("Bad files matching " + pattern + " in " + dir,
        Joiner.on(",").join(expectedSet),
        Joiner.on(",").join(found));
  }

  static final String E_NULL_THROWABLE = "Null Throwable";
  static final String E_NULL_THROWABLE_STRING =
      "Null Throwable.toString() value";
  static final String E_UNEXPECTED_EXCEPTION = "but got unexpected exception";

  /**
   * Assert that an exception's <code>toString()</code> value
   * contained the expected text.
   * @param string expected string
   * @param t thrown exception
   * @throws AssertionError if the expected string is not found
   */
  public static void assertExceptionContains(String string, Throwable t) {
    Assert.assertNotNull(E_NULL_THROWABLE, t);
    String msg = t.toString();
    if (msg == null) {
      throw new AssertionError(E_NULL_THROWABLE_STRING, t);
    }
    if (!msg.contains(string)) {
      throw new AssertionError("Expected to find '" + string + "' "
          + E_UNEXPECTED_EXCEPTION + ":"
          + StringUtils.stringifyException(t),
          t);
    }
  }  

  public static void waitFor(Supplier<Boolean> check,
      int checkEveryMillis, int waitForMillis)
      throws TimeoutException, InterruptedException
  {
    long st = Time.now();
    do {
      boolean result = check.get();
      if (result) {
        return;
      }
      
      Thread.sleep(checkEveryMillis);
    } while (Time.now() - st < waitForMillis);
    
    throw new TimeoutException("Timed out waiting for condition. " +
        "Thread diagnostics:\n" +
        TimedOutTestsListener.buildThreadDiagnosticString());
  }

  /**
   * Prints output to one {@link PrintStream} while copying to the other.
   * <p>
   * Closing the main {@link PrintStream} will NOT close the other.
   */
  public static class TeePrintStream extends PrintStream {
    private final PrintStream other;

    public TeePrintStream(OutputStream main, PrintStream other) {
      super(main);
      this.other = other;
    }

    @Override
    public void flush() {
      super.flush();
      other.flush();
    }

    @Override
    public void write(byte[] buf, int off, int len) {
      super.write(buf, off, len);
      other.write(buf, off, len);
    }
  }

  /**
   * Capture output printed to {@link System#err}.
   * <p>
   * Usage:
   * <pre>
   *   try (SystemErrCapturer capture = new SystemErrCapturer()) {
   *     ...
   *     // Call capture.getOutput() to get the output string
   *   }
   * </pre>
   *
   * TODO: Add lambda support once Java 8 is common.
   * <pre>
   *   SystemErrCapturer.withCapture(capture -> {
   *     ...
   *   })
   * </pre>
   */
  public static class SystemErrCapturer implements AutoCloseable {
    final private ByteArrayOutputStream bytes;
    final private PrintStream bytesPrintStream;
    final private PrintStream oldErr;

    public SystemErrCapturer() {
      bytes = new ByteArrayOutputStream();
      bytesPrintStream = new PrintStream(bytes);
      oldErr = System.err;
      System.setErr(new TeePrintStream(oldErr, bytesPrintStream));
    }

    public String getOutput() {
      return bytes.toString();
    }

    @Override
    public void close() throws Exception {
      IOUtils.closeQuietly(bytesPrintStream);
      System.setErr(oldErr);
    }
  }

  public static class LogCapturer {
    private StringWriter sw = new StringWriter();
    private WriterAppender appender;
    private Logger logger;

    public static LogCapturer captureLogs(Log l) {
      Logger logger = ((Log4JLogger)l).getLogger();
      return new LogCapturer(logger);
    }

    public static LogCapturer captureLogs(org.slf4j.Logger logger) {
      return new LogCapturer(toLog4j(logger));
    }

    private LogCapturer(Logger logger) {
      this.logger = logger;
      Appender defaultAppender = Logger.getRootLogger().getAppender("stdout");
      if (defaultAppender == null) {
        defaultAppender = Logger.getRootLogger().getAppender("console");
      }
      final Layout layout = (defaultAppender == null) ? new PatternLayout() :
          defaultAppender.getLayout();
      this.appender = new WriterAppender(layout, sw);
      logger.addAppender(this.appender);
    }

    public String getOutput() {
      return sw.toString();
    }

    public void stopCapturing() {
      logger.removeAppender(appender);
    }

    public void clearOutput() {
      sw.getBuffer().setLength(0);
    }
  }

  /**
   * Mockito answer helper that triggers one latch as soon as the
   * method is called, then waits on another before continuing.
   */
  public static class DelayAnswer implements Answer<Object> {
    private final Log LOG;
    
    private final CountDownLatch fireLatch = new CountDownLatch(1);
    private final CountDownLatch waitLatch = new CountDownLatch(1);
    private final CountDownLatch resultLatch = new CountDownLatch(1);
    
    private final AtomicInteger fireCounter = new AtomicInteger(0);
    private final AtomicInteger resultCounter = new AtomicInteger(0);
    
    // Result fields set after proceed() is called.
    private volatile Throwable thrown;
    private volatile Object returnValue;
    
    public DelayAnswer(Log log) {
      this.LOG = log;
    }

    /**
     * Wait until the method is called.
     */
    public void waitForCall() throws InterruptedException {
      fireLatch.await();
    }
  
    /**
     * Tell the method to proceed.
     * This should only be called after waitForCall()
     */
    public void proceed() {
      waitLatch.countDown();
    }
  
    @Override
    public Object answer(InvocationOnMock invocation) throws Throwable {
      LOG.info("DelayAnswer firing fireLatch");
      fireCounter.getAndIncrement();
      fireLatch.countDown();
      try {
        LOG.info("DelayAnswer waiting on waitLatch");
        waitLatch.await();
        LOG.info("DelayAnswer delay complete");
      } catch (InterruptedException ie) {
        throw new IOException("Interrupted waiting on latch", ie);
      }
      return passThrough(invocation);
    }

    protected Object passThrough(InvocationOnMock invocation) throws Throwable {
      try {
        Object ret = invocation.callRealMethod();
        returnValue = ret;
        return ret;
      } catch (Throwable t) {
        thrown = t;
        throw t;
      } finally {
        resultCounter.incrementAndGet();
        resultLatch.countDown();
      }
    }
    
    /**
     * After calling proceed(), this will wait until the call has
     * completed and a result has been returned to the caller.
     */
    public void waitForResult() throws InterruptedException {
      resultLatch.await();
    }
    
    /**
     * After the call has gone through, return any exception that
     * was thrown, or null if no exception was thrown.
     */
    public Throwable getThrown() {
      return thrown;
    }
    
    /**
     * After the call has gone through, return the call's return value,
     * or null in case it was void or an exception was thrown.
     */
    public Object getReturnValue() {
      return returnValue;
    }
    
    public int getFireCount() {
      return fireCounter.get();
    }
    
    public int getResultCount() {
      return resultCounter.get();
    }
  }
  
  /**
   * An Answer implementation that simply forwards all calls through
   * to a delegate.
   * 
   * This is useful as the default Answer for a mock object, to create
   * something like a spy on an RPC proxy. For example:
   * <code>
   *    NamenodeProtocol origNNProxy = secondary.getNameNode();
   *    NamenodeProtocol spyNNProxy = Mockito.mock(NameNodeProtocol.class,
   *        new DelegateAnswer(origNNProxy);
   *    doThrow(...).when(spyNNProxy).getBlockLocations(...);
   *    ...
   * </code>
   */
  public static class DelegateAnswer implements Answer<Object> { 
    private final Object delegate;
    private final Log log;
    
    public DelegateAnswer(Object delegate) {
      this(null, delegate);
    }
    
    public DelegateAnswer(Log log, Object delegate) {
      this.log = log;
      this.delegate = delegate;
    }

    @Override
    public Object answer(InvocationOnMock invocation) throws Throwable {
      try {
        if (log != null) {
          log.info("Call to " + invocation + " on " + delegate,
              new Exception("TRACE"));
        }
        return invocation.getMethod().invoke(
            delegate, invocation.getArguments());
      } catch (InvocationTargetException ite) {
        throw ite.getCause();
      }
    }
  }

  /**
   * An Answer implementation which sleeps for a random number of milliseconds
   * between 0 and a configurable value before delegating to the real
   * implementation of the method. This can be useful for drawing out race
   * conditions.
   */
  public static class SleepAnswer implements Answer<Object> {
    private final int maxSleepTime;
    private static Random r = new Random();
    
    public SleepAnswer(int maxSleepTime) {
      this.maxSleepTime = maxSleepTime;
    }
    
    @Override
    public Object answer(InvocationOnMock invocation) throws Throwable {
      boolean interrupted = false;
      try {
        Thread.sleep(r.nextInt(maxSleepTime));
      } catch (InterruptedException ie) {
        interrupted = true;
      }
      try {
        return invocation.callRealMethod();
      } finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  public static void assertDoesNotMatch(String output, String pattern) {
    Assert.assertFalse("Expected output to match /" + pattern + "/" +
        " but got:\n" + output,
        Pattern.compile(pattern).matcher(output).find());
  }

  public static void assertMatches(String output, String pattern) {
    Assert.assertTrue("Expected output to match /" + pattern + "/" +
        " but got:\n" + output,
        Pattern.compile(pattern).matcher(output).find());
  }
  
  public static void assertValueNear(long expected, long actual, long allowedError) {
    assertValueWithinRange(expected - allowedError, expected + allowedError, actual);
  }
  
  public static void assertValueWithinRange(long expectedMin, long expectedMax,
      long actual) {
    Assert.assertTrue("Expected " + actual + " to be in range (" + expectedMin + ","
        + expectedMax + ")", expectedMin <= actual && actual <= expectedMax);
  }

  /**
   * Determine if there are any threads whose name matches the regex.
   * @param pattern a Pattern object used to match thread names
   * @return true if there is any thread that matches the pattern
   */
  public static boolean anyThreadMatching(Pattern pattern) {
    ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();

    ThreadInfo[] infos =
        threadBean.getThreadInfo(threadBean.getAllThreadIds(), 20);
    for (ThreadInfo info : infos) {
      if (info == null)
        continue;
      if (pattern.matcher(info.getThreadName()).matches()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Assert that there are no threads running whose name matches the
   * given regular expression.
   * @param regex the regex to match against
   */
  public static void assertNoThreadsMatching(String regex) {
    Pattern pattern = Pattern.compile(regex);
    if (anyThreadMatching(pattern)) {
      Assert.fail("Leaked thread matches " + regex);
    }
  }

  /**
   * Periodically check and wait for any threads whose name match the
   * given regular expression.
   *
   * @param regex the regex to match against.
   * @param checkEveryMillis time (in milliseconds) between checks.
   * @param waitForMillis total time (in milliseconds) to wait before throwing
   *                      a time out exception.
   * @throws TimeoutException
   * @throws InterruptedException
   */
  public static void waitForThreadTermination(String regex,
      int checkEveryMillis, final int waitForMillis) throws TimeoutException,
      InterruptedException {
    final Pattern pattern = Pattern.compile(regex);
    waitFor(new Supplier<Boolean>() {
      @Override public Boolean get() {
        return !anyThreadMatching(pattern);
      }
    }, checkEveryMillis, waitForMillis);
  }


  /**
   * Skip test if native build profile of Maven is not activated.
   * Sub-project using this must set 'runningWithNative' property to true
   * in the definition of native profile in pom.xml.
   */
  public static void assumeInNativeProfile() {
    Assume.assumeTrue(
        Boolean.parseBoolean(System.getProperty("runningWithNative", "false")));
  }

  /**
   * Get the diff between two files.
   *
   * @param a
   * @param b
   * @return The empty string if there is no diff; the diff, otherwise.
   *
   * @throws IOException If there is an error reading either file.
   */
  public static String getFilesDiff(File a, File b) throws IOException {
    StringBuilder bld = new StringBuilder();
    BufferedReader ra = null, rb = null;
    try {
      ra = new BufferedReader(
          new InputStreamReader(new FileInputStream(a)));
      rb = new BufferedReader(
          new InputStreamReader(new FileInputStream(b)));
      while (true) {
        String la = ra.readLine();
        String lb = rb.readLine();
        if (la == null) {
          if (lb != null) {
            addPlusses(bld, ra);
          }
          break;
        } else if (lb == null) {
          if (la != null) {
            addPlusses(bld, rb);
          }
          break;
        }
        if (!la.equals(lb)) {
          bld.append(" - ").append(la).append("\n");
          bld.append(" + ").append(lb).append("\n");
        }
      }
    } finally {
      IOUtils.closeQuietly(ra);
      IOUtils.closeQuietly(rb);
    }
    return bld.toString();
  }

  private static void addPlusses(StringBuilder bld, BufferedReader r)
      throws IOException {
    String l;
    while ((l = r.readLine()) != null) {
      bld.append(" + ").append(l).append("\n");
    }
  }
}
