/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.smartdata.versioninfo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;

public class VersionInfoWrite {
  File directory = new File("");
  String pom = directory.getAbsolutePath() + "/" + "pom.xml";

  public void execute() {
    Properties prop = new Properties();
    OutputStream output = null;
    String s = this.getClass().getResource("/").getPath() + "common-versionInfo.properties";
    try {
      output = new FileOutputStream(s);
      prop.setProperty("version", getVersionInfo(pom));
      prop.setProperty("revision", getCommit());
      prop.setProperty("user", getUser());
      prop.setProperty("date", getBuildTime());
      prop.setProperty("url", getUri());
      prop.setProperty("branch", getBranch());
      prop.store(output, new Date().toString());
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (output != null) {
        try {
          output.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  private String getBuildTime() {
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
    dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    return dateFormat.format(new Date());
  }

  private String getVersionInfo(String pom) throws IOException {
    File file = new File(pom);
    FileReader fileReader = new FileReader(file);
    String st;
    BufferedReader br = new BufferedReader(fileReader);
    while ((st = br.readLine()) != null) {
      if (st.contains("<version>")) {
        return st.trim().substring("<version>".length(),
            st.trim().length() - "</version>".length());
      }
    }
    return "Not found";
  }

  private String getUri() {
    List<String> list = execCmd("git remote -v");
    String uri = "Unknown";
    for (String s : list) {
      if (s.startsWith("origin") && s.endsWith("(fetch)")) {
        uri = s.substring("origin".length());
        uri = uri.substring(0, uri.length() - "(fetch)".length());
        break;
      }
    }
    return uri.trim();
  }

  private String getCommit() {
    List<String> list = execCmd("git log -n 1");
    String commit = "Unknown";
    for (String s : list) {
      if (s.startsWith("commit")) {
        commit = s.substring("commit".length());
        break;
      }
    }
    return commit.trim();
  }

  private String getUser() {
    List<String> list = execCmd("whoami");
    String user = "Unknown";
    for (String s : list) {
      user = s.trim();
      break;
    }
    return user;
  }

  private String getBranch() {
    List<String> list = execCmd("git branch");
    String branch = "Unknown";
    for (String s : list) {
      if (s.startsWith("*")) {
        branch = s.substring("*".length()).trim();
        break;
      }
    }
    return branch;
  }

  public List<String> execCmd(String cmd) {
    String command = "/bin/sh -c " + cmd;
    List<String> list = new ArrayList<String>();
    try {
      Runtime rt = Runtime.getRuntime();
      Process proc = rt.exec(cmd, null, null);
      InputStream stderr = proc.getInputStream();
      InputStreamReader isr = new InputStreamReader(stderr, "UTF-8");
      BufferedReader br = new BufferedReader(isr);
      String line;
      while ((line = br.readLine()) != null) {
        list.add(line);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return list;
  }

  public static void main(String[] args) {
    VersionInfoWrite w = new VersionInfoWrite();
    w.execute();
  }
}
