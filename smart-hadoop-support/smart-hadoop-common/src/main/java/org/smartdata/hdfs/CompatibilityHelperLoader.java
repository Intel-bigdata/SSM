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
package org.smartdata.hdfs;

import org.apache.hadoop.util.VersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompatibilityHelperLoader {
  private static CompatibilityHelper instance;
  private static final String HADOOP_26_HELPER_CLASS = "org.smartdata.hdfs.CompatibilityHelper26";
  private static final String HADOOP_27_HELPER_CLASS = "org.smartdata.hdfs.CompatibilityHelper27";
  private static final String HADOOP_31_HELPER_CLASS = "org.smartdata.hdfs.CompatibilityHelper31";
  public static final Logger LOG =
      LoggerFactory.getLogger(CompatibilityHelperLoader.class);

  public static CompatibilityHelper getHelper() {
    if (instance == null) {
      String version = VersionInfo.getVersion();
      if (version == null || version.isEmpty() || version.equalsIgnoreCase("Unknown")) {
        LOG.error("Cannot get Hadoop version. Use default 2.6.3 version. ");
        version = "2.6.3";
      }
      String[] parts = version.split("\\.");
      if (parts.length < 2) {
        throw new RuntimeException("Illegal Hadoop Version: " + version + " (expected A.B.* format)");
      }
      Integer first = Integer.parseInt(parts[0]);
      if (first == 0 || first == 1) {
        throw new RuntimeException("Hadoop version 0.x and 1.x are not supported");
      }
      Integer second = Integer.parseInt(parts[1]);
      if (first == 2 && second <= 6) {
        instance = create(HADOOP_26_HELPER_CLASS);
      } else if (first == 2 && second <= 9){
        instance = create(HADOOP_27_HELPER_CLASS);
      } else {
        instance = create(HADOOP_31_HELPER_CLASS);
      }
    }
    return instance;
  }

  private static CompatibilityHelper create(String classString) {
    try {
      ClassLoader loader = Thread.currentThread().getContextClassLoader();
      if (loader == null) {
        loader = ClassLoader.getSystemClassLoader();
      }
      Class clazz = loader.loadClass(classString);
      return (CompatibilityHelper) clazz.newInstance();
    } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
}
