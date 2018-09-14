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
package org.smartdata.versioninfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class VersionInfoRead {
  Properties prop = new Properties();

  public static final Logger LOG =
      LoggerFactory.getLogger(VersionInfoRead.class);

  public VersionInfoRead(String component) {
    InputStream in = null;
    String s = component + "-versionInfo.properties";
    try {
      in = Thread.currentThread().getContextClassLoader().getResourceAsStream(s);
      prop.load(in);
    } catch (Exception e) {
      LOG.error("" + e);
    } finally {
      try {
        in.close();
      } catch (IOException e) {
        LOG.error(e.getLocalizedMessage());
      }
    }
  }

  public Properties getProperties() {
    return prop;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (String p : prop.stringPropertyNames()) {
      sb.append(p).append("=").append(prop.getProperty(p)).append("\n");
    }
    return sb.toString();
  }
}
