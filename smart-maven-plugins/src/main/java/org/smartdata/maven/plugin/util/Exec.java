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
package org.smartdata.maven.plugin.util;

import org.apache.maven.plugin.Mojo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class Exec {
  private Mojo mojo;

  public Exec(Mojo mojo) {
    this.mojo = mojo;
  }

  public int run(List<String> command, List<String> output) {
    return this.run(command, output, (List) null);
  }

  public int run(List<String> command, List<String> output, List<String> errors) {
    int retCode = 1;
    ProcessBuilder pb = new ProcessBuilder(command);

    try {
      Process p = pb.start();
      Exec.OutputBufferThread stdOut = new Exec.OutputBufferThread(p.getInputStream());
      Exec.OutputBufferThread stdErr = new Exec.OutputBufferThread(p.getErrorStream());
      stdOut.start();
      stdErr.start();
      retCode = p.waitFor();
      if (retCode != 0) {
        this.mojo.getLog().warn(command + " failed with error code " + retCode);
        Iterator var9 = stdErr.getOutput().iterator();

        while (var9.hasNext()) {
          String s = (String) var9.next();
          this.mojo.getLog().debug(s);
        }
      }

      stdOut.join();
      stdErr.join();
      output.addAll(stdOut.getOutput());
      if (errors != null) {
        errors.addAll(stdErr.getOutput());
      }
    } catch (IOException var11) {
      this.mojo.getLog().warn(command + " failed: " + var11.toString());
    } catch (InterruptedException var12) {
      this.mojo.getLog().warn(command + " failed: " + var12.toString());
    }

    return retCode;
  }

  public static void addEnvironment(ProcessBuilder pb, Map<String, String> env) {
    if (env != null) {
      Map<String, String> processEnv = pb.environment();

      Entry entry;
      String val;
      for (Iterator var3 = env.entrySet().iterator();
          var3.hasNext();
          processEnv.put((String) entry.getKey(), val)) {
        entry = (Entry) var3.next();
        val = (String) entry.getValue();
        if (val == null) {
          val = "";
        }
      }
    }
  }

  public static String envToString(Map<String, String> env) {
    StringBuilder bld = new StringBuilder();
    bld.append("{");
    Entry entry;
    String val;
    if (env != null) {
      for (Iterator var2 = env.entrySet().iterator();
          var2.hasNext();
          bld.append("\n  ")
              .append((String) entry.getKey())
              .append(" = '")
              .append(val)
              .append("'\n")) {
        entry = (Entry) var2.next();
        val = (String) entry.getValue();
        if (val == null) {
          val = "";
        }
      }
    }

    bld.append("}");
    return bld.toString();
  }

  public static class OutputBufferThread extends Thread {
    private List<String> output;
    private BufferedReader reader;

    public OutputBufferThread(InputStream is) {
      this.setDaemon(true);
      this.output = new ArrayList();

      try {
        this.reader = new BufferedReader(new InputStreamReader(is, "UTF-8"));
      } catch (UnsupportedEncodingException var3) {
        throw new RuntimeException("Unsupported encoding " + var3.toString());
      }
    }

    public void run() {
      try {
        for (String line = this.reader.readLine(); line != null; line = this.reader.readLine()) {
          this.output.add(line);
        }

      } catch (IOException var2) {
        throw new RuntimeException("make failed with error code " + var2.toString());
      }
    }

    public List<String> getOutput() {
      return this.output;
    }
  }
}
