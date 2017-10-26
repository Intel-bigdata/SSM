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

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.smartdata.maven.plugin.util.Exec;
import org.smartdata.maven.plugin.util.Exec.OutputBufferThread;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

@Mojo(name = "cmake-compile", defaultPhase = LifecyclePhase.COMPILE)
public class CompileMojo extends AbstractMojo {
  private static int availableProcessors = Runtime.getRuntime().availableProcessors();

  @Parameter(defaultValue = "${project.build.directory}/native")
  private File output;

  @Parameter(defaultValue = "${basedir}/src/main/native", required = true)
  private File source;

  @Parameter private String target;
  @Parameter private Map<String, String> env;
  @Parameter private Map<String, String> vars;

  public CompileMojo() {}

  private static void validatePlatform() throws MojoExecutionException {
    if (System.getProperty("os.name").toLowerCase(Locale.ENGLISH).startsWith("windows")) {
      throw new MojoExecutionException("CMakeBuilder does not yet support the Windows platform.");
    }
  }

  public void execute() throws MojoExecutionException {
    long start = System.nanoTime();
    validatePlatform();
    this.runCMake();
    this.runMake();
    this.runMake();
    long end = System.nanoTime();
    this.getLog()
        .info(
            "cmake compilation finished successfully in "
                + TimeUnit.MILLISECONDS.convert(end - start, TimeUnit.NANOSECONDS)
                + " millisecond(s).");
  }

  static void validateSourceParams(File source, File output) throws MojoExecutionException {
    String cOutput = null;
    String cSource = null;

    try {
      cOutput = output.getCanonicalPath();
    } catch (IOException var6) {
      throw new MojoExecutionException("error getting canonical path for output", var6);
    }

    try {
      cSource = source.getCanonicalPath();
    } catch (IOException var5) {
      throw new MojoExecutionException("error getting canonical path for source", var5);
    }

    if (cSource.startsWith(cOutput)) {
      throw new MojoExecutionException(
          "The source directory must not be inside the output directory "
              + "(it would be destroyed by 'mvn clean')");
    }
  }

  public void runCMake() throws MojoExecutionException {
    validatePlatform();
    validateSourceParams(this.source, this.output);
    if (this.output.mkdirs()) {
      this.getLog().info("mkdirs '" + this.output + "'");
    }

    List<String> cmd = new LinkedList();
    cmd.add("cmake");
    cmd.add(this.source.getAbsolutePath());
    Iterator var2 = this.vars.entrySet().iterator();

    while (var2.hasNext()) {
      Entry<String, String> entry = (Entry) var2.next();
      if (entry.getValue() != null && !((String) entry.getValue()).equals("")) {
        cmd.add("-D" + (String) entry.getKey() + "=" + (String) entry.getValue());
      }
    }

    cmd.add("-G");
    cmd.add("Unix Makefiles");
    String prefix = "";
    StringBuilder bld = new StringBuilder();

    for (Iterator var4 = cmd.iterator(); var4.hasNext(); prefix = " ") {
      String c = (String) var4.next();
      bld.append(prefix).append(c);
    }

    this.getLog().info("Running " + bld.toString());
    this.getLog().info("with extra environment variables " + Exec.envToString(this.env));
    ProcessBuilder pb = new ProcessBuilder(cmd);
    pb.directory(this.output);
    pb.redirectErrorStream(true);
    Exec.addEnvironment(pb, this.env);
    Process proc = null;
    OutputBufferThread outThread = null;
    int retCode = -1;
    boolean var18 = false;

    try {
      var18 = true;
      proc = pb.start();
      outThread = new OutputBufferThread(proc.getInputStream());
      outThread.start();
      retCode = proc.waitFor();
      if (retCode != 0) {
        throw new MojoExecutionException("CMake failed with error code " + retCode);
      }

      var18 = false;
    } catch (IOException var21) {
      throw new MojoExecutionException("Error executing CMake", var21);
    } catch (InterruptedException var22) {
      throw new MojoExecutionException("Interrupted while waiting for CMake process", var22);
    } finally {
      if (var18) {
        if (proc != null) {
          proc.destroy();
        }

        if (outThread != null) {
          try {
            outThread.interrupt();
            outThread.join();
          } catch (InterruptedException var19) {
            this.getLog().error("Interrupted while joining output thread", var19);
          }

          if (retCode != 0) {
            Iterator var11 = outThread.getOutput().iterator();

            while (var11.hasNext()) {
              String line = (String) var11.next();
              this.getLog().warn(line);
            }
          }
        }
      }
    }

    if (proc != null) {
      proc.destroy();
    }

    if (outThread != null) {
      try {
        outThread.interrupt();
        outThread.join();
      } catch (InterruptedException var20) {
        this.getLog().error("Interrupted while joining output thread", var20);
      }

      if (retCode != 0) {
        Iterator var8 = outThread.getOutput().iterator();

        while (var8.hasNext()) {
          String line = (String) var8.next();
          this.getLog().warn(line);
        }
      }
    }
  }

  public void runMake() throws MojoExecutionException {
    List<String> cmd = new LinkedList();
    cmd.add("make");
    cmd.add("-j");
    cmd.add(String.valueOf(availableProcessors));
    cmd.add("VERBOSE=1");
    if (this.target != null) {
      cmd.add(this.target);
    }

    StringBuilder bld = new StringBuilder();
    String prefix = "";

    for (Iterator var4 = cmd.iterator(); var4.hasNext(); prefix = " ") {
      String c = (String) var4.next();
      bld.append(prefix).append(c);
    }

    this.getLog().info("Running " + bld.toString());
    ProcessBuilder pb = new ProcessBuilder(cmd);
    pb.directory(this.output);
    Process proc = null;
    int retCode = -1;
    OutputBufferThread stdoutThread = null;
    OutputBufferThread stderrThread = null;
    boolean var21 = false;

    try {
      var21 = true;
      proc = pb.start();
      stdoutThread = new OutputBufferThread(proc.getInputStream());
      stderrThread = new OutputBufferThread(proc.getErrorStream());
      stdoutThread.start();
      stderrThread.start();
      retCode = proc.waitFor();
      if (retCode != 0) {
        throw new MojoExecutionException("make failed with error code " + retCode);
      }

      var21 = false;
    } catch (InterruptedException var26) {
      throw new MojoExecutionException("Interrupted during Process#waitFor", var26);
    } catch (IOException var27) {
      throw new MojoExecutionException("Error executing make", var27);
    } finally {
      if (var21) {
        Iterator var12;
        String line;
        if (stdoutThread != null) {
          try {
            stdoutThread.join();
          } catch (InterruptedException var23) {
            this.getLog().error("Interrupted while joining stdoutThread", var23);
          }

          if (retCode != 0) {
            var12 = stdoutThread.getOutput().iterator();

            while (var12.hasNext()) {
              line = (String) var12.next();
              this.getLog().warn(line);
            }
          }
        }

        if (stderrThread != null) {
          try {
            stderrThread.join();
          } catch (InterruptedException var22) {
            this.getLog().error("Interrupted while joining stderrThread", var22);
          }

          var12 = stderrThread.getOutput().iterator();

          while (var12.hasNext()) {
            line = (String) var12.next();
            this.getLog().warn(line);
          }
        }

        if (proc != null) {
          proc.destroy();
        }
      }
    }

    Iterator var9;
    String line;
    if (stdoutThread != null) {
      try {
        stdoutThread.join();
      } catch (InterruptedException var25) {
        this.getLog().error("Interrupted while joining stdoutThread", var25);
      }

      if (retCode != 0) {
        var9 = stdoutThread.getOutput().iterator();

        while (var9.hasNext()) {
          line = (String) var9.next();
          this.getLog().warn(line);
        }
      }
    }

    if (stderrThread != null) {
      try {
        stderrThread.join();
      } catch (InterruptedException var24) {
        this.getLog().error("Interrupted while joining stderrThread", var24);
      }

      var9 = stderrThread.getOutput().iterator();

      while (var9.hasNext()) {
        line = (String) var9.next();
        this.getLog().warn(line);
      }
    }

    if (proc != null) {
      proc.destroy();
    }
  }
}
