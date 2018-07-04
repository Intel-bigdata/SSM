/**
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */
package org.smartdata.versioninfo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class VersionInfoRead {
  public void printInfo() throws IOException {
    String s = this.getClass().getResource("/").getPath()+ "/../../src/main/resources/versionInfo.properties";
    FileReader v = new FileReader(s);
    BufferedReader br = new BufferedReader(v);
    String version = "Not found";
    String url = "Not found";
    String revision = "Not found";
    String time = "Not found";
    String user = "Not found";
    String st = null;
    while ((st = br.readLine()) != null) {
      if (st.contains("user")) {
        user = st.substring("user=".length());
      }
      if (st.contains("date")) {
        time = st.substring("date=".length());
      }
      if (st.contains("revision")) {
        revision = st.substring("revision=".length());
      }
      if (st.contains("url")) {
        url = st.substring("url=".length());
      }
      if (st.contains("version")) {
        version = st.substring("version=".length());
      }
    }
    v.close();
    br.close();
    System.out.println("SSM " + version);
    System.out.println("Subversion " + url + " -r " + revision);
    System.out.println("Compiled by " + user + " on " + time);
  }

  public static void main(String[] args) {
    VersionInfoRead t = new VersionInfoRead();
    try {
      t.printInfo();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
