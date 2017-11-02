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
package org.smartdata.maven.plugin.cmakebuilder;

import org.apache.maven.execution.MavenSession;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.smartdata.maven.plugin.util.Exec;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

@Mojo(name = "cmake-test", defaultPhase = LifecyclePhase.TEST)
public class TestMojo extends AbstractMojo {
  private static final String ALL_NATIVE = "allNative";

  @Parameter(required = true)
  private File binary;

  @Parameter private String testName;
  @Parameter private Map<String, String> env;
  @Parameter private List<String> args = new LinkedList();

  @Parameter(defaultValue = "600")
  private int timeout;

  @Parameter private File workingDirectory;

  @Parameter(defaultValue = "native-results")
  private File results;

  @Parameter private Map<String, String> preconditions = new HashMap();

  @Parameter(defaultValue = "false")
  private boolean skipIfMissing;

  @Parameter(defaultValue = "success")
  private String expectedResult;

  @Parameter(defaultValue = "${session}", readonly = true, required = true)
  private MavenSession session;

  private static final String VALID_PRECONDITION_TYPES_STR =
      "Valid precondition types are \"and\", \"andNot\"";

  public TestMojo() {}

  private static void validatePlatform() throws MojoExecutionException {
    if (System.getProperty("os.name").toLowerCase(Locale.ENGLISH).startsWith("windows")) {
      throw new MojoExecutionException("CMakeBuilder does not yet support the Windows platform.");
    }
  }

  private void writeStatusFile(String status) throws IOException {
    FileOutputStream fos = new FileOutputStream(new File(this.results, this.testName + ".pstatus"));
    BufferedWriter out = null;

    try {
      out = new BufferedWriter(new OutputStreamWriter(fos, "UTF8"));
      out.write(status + "\n");
    } finally {
      if (out != null) {
        out.close();
      } else {
        fos.close();
      }
    }
  }

  private static boolean isTruthy(String str) {
    return str == null
        ? false
        : (str.equalsIgnoreCase("")
            ? false
            : (str.equalsIgnoreCase("false")
                ? false
                : (str.equalsIgnoreCase("no")
                    ? false
                    : (str.equalsIgnoreCase("off") ? false : !str.equalsIgnoreCase("disable")))));
  }

  private void validateParameters() throws MojoExecutionException {
    if (!this.expectedResult.equals("success")
        && !this.expectedResult.equals("failure")
        && !this.expectedResult.equals("any")) {
      throw new MojoExecutionException("expectedResult must be either success, failure, or any");
    }
  }

  private boolean shouldRunTest() throws MojoExecutionException {
    String skipTests = this.session.getSystemProperties().getProperty("skipTests");
    if (isTruthy(skipTests)) {
      this.getLog().info("skipTests is in effect for test " + this.testName);
      return false;
    } else if (!this.binary.exists()) {
      if (this.skipIfMissing) {
        this.getLog().info("Skipping missing test " + this.testName);
        return false;
      } else {
        throw new MojoExecutionException(
            "Test " + this.binary + " was not built!  (File does not exist.)");
      }
    } else {
      String testProp = this.session.getSystemProperties().getProperty("test");
      if (testProp != null) {
        String[] testPropArr = testProp.split(",");
        boolean found = false;
        String[] var5 = testPropArr;
        int var6 = testPropArr.length;

        for (int var7 = 0; var7 < var6; ++var7) {
          String test = var5[var7];
          if (test.equals("allNative")) {
            found = true;
            break;
          }

          if (test.equals(this.testName)) {
            found = true;
            break;
          }
        }

        if (!found) {
          this.getLog().debug("did not find test '" + this.testName + "' in list " + testProp);
          return false;
        }
      }

      if (this.preconditions != null) {
        int idx = 1;

        for (Iterator var10 = this.preconditions.entrySet().iterator(); var10.hasNext(); ++idx) {
          Entry<String, String> entry = (Entry) var10.next();
          String key = (String) entry.getKey();
          String val = (String) entry.getValue();
          if (key == null) {
            throw new MojoExecutionException(
                "NULL is not a valid precondition type. "
                    + "Valid precondition types are \"and\", \"andNot\"");
          }

          if (key.equals("and")) {
            if (!isTruthy(val)) {
              this.getLog()
                  .info(
                      "Skipping test "
                          + this.testName
                          + " because precondition number "
                          + idx
                          + " was not met.");
              return false;
            }
          } else {
            if (!key.equals("andNot")) {
              throw new MojoExecutionException(
                  key
                      + " is not a valid precondition type.  "
                      + "Valid precondition types are \"and\", \"andNot\"");
            }

            if (isTruthy(val)) {
              this.getLog()
                  .info(
                      "Skipping test "
                          + this.testName
                          + " because negative precondition number "
                          + idx
                          + " was met.");
              return false;
            }
          }
        }
      }

      return true;
    }
  }

  public void execute() throws MojoExecutionException {
    if (this.testName == null) {
      this.testName = this.binary.getName();
    }

    validatePlatform();
    this.validateParameters();
    if (this.shouldRunTest()) {
      if (!this.results.isDirectory() && !this.results.mkdirs()) {
        throw new MojoExecutionException(
            "Failed to create output directory '" + this.results + "'!");
      } else {
        List<String> cmd = new LinkedList();
        cmd.add(this.binary.getAbsolutePath());
        this.getLog().info("-------------------------------------------------------");
        this.getLog().info(" C M A K E B U I L D E R    T E S T");
        this.getLog().info("-------------------------------------------------------");
        StringBuilder bld = new StringBuilder();
        bld.append(this.testName).append(": running ");
        bld.append(this.binary.getAbsolutePath());
        Iterator var3 = this.args.iterator();

        while (var3.hasNext()) {
          String entry = (String) var3.next();
          cmd.add(entry);
          bld.append(" ").append(entry);
        }

        this.getLog().info(bld.toString());
        ProcessBuilder pb = new ProcessBuilder(cmd);
        Exec.addEnvironment(pb, this.env);
        if (this.workingDirectory != null) {
          pb.directory(this.workingDirectory);
        }

        pb.redirectError(new File(this.results, this.testName + ".stderr"));
        pb.redirectOutput(new File(this.results, this.testName + ".stdout"));
        this.getLog().info("with extra environment variables " + Exec.envToString(this.env));
        Process proc = null;
        TestMojo.TestThread testThread = null;
        int retCode = -1;
        String status = "IN_PROGRESS";

        try {
          this.writeStatusFile(status);
        } catch (IOException var23) {
          throw new MojoExecutionException("Error writing the status file", var23);
        }

        long start = System.nanoTime();

        try {
          proc = pb.start();
          testThread = new TestMojo.TestThread(proc);
          testThread.start();
          testThread.join((long) (this.timeout * 1000));
          if (!testThread.isAlive()) {
            retCode = testThread.retCode();
            testThread = null;
            proc = null;
          }
        } catch (IOException var24) {
          throw new MojoExecutionException(
              "IOException while executing the test " + this.testName, var24);
        } catch (InterruptedException var25) {
          throw new MojoExecutionException(
              "Interrupted while executing the test " + this.testName, var25);
        } finally {
          if (testThread != null) {
            testThread.interrupt();

            try {
              testThread.join();
            } catch (InterruptedException var22) {
              this.getLog().error("Interrupted while waiting for testThread", var22);
            }

            status = "TIMED OUT";
          } else if (retCode == 0) {
            status = "SUCCESS";
          } else {
            status = "ERROR CODE " + String.valueOf(retCode);
          }

          try {
            this.writeStatusFile(status);
          } catch (Exception var21) {
            this.getLog().error("failed to write status file!", var21);
          }

          if (proc != null) {
            proc.destroy();
          }
        }

        long end = System.nanoTime();
        this.getLog()
            .info(
                "STATUS: "
                    + status
                    + " after "
                    + TimeUnit.MILLISECONDS.convert(end - start, TimeUnit.NANOSECONDS)
                    + " millisecond(s).");
        this.getLog().info("-------------------------------------------------------");
        if (status.equals("TIMED_OUT")) {
          if (this.expectedResult.equals("success")) {
            throw new MojoExecutionException(
                "Test " + this.binary + " timed out after " + this.timeout + " seconds!");
          }
        } else if (!status.equals("SUCCESS")) {
          if (this.expectedResult.equals("success")) {
            throw new MojoExecutionException("Test " + this.binary + " returned " + status);
          }
        } else if (this.expectedResult.equals("failure")) {
          throw new MojoExecutionException(
              "Test " + this.binary + " succeeded, but we expected failure!");
        }
      }
    }
  }

  private static class TestThread extends Thread {
    private Process proc;
    private int retCode = -1;

    public TestThread(Process proc) {
      this.proc = proc;
    }

    public void run() {
      try {
        this.retCode = this.proc.waitFor();
      } catch (InterruptedException var2) {
        this.retCode = -1;
      }
    }

    public int retCode() {
      return this.retCode;
    }
  }
}
