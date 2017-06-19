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
package org.smartdata.server;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.conf.ReconfigureException;

import java.util.List;
import java.util.Set;

public class ReconfigurableRegistry {
  private static ListMultimap<String, Reconfigurable> reconfMap =
      ArrayListMultimap.create();
  public static final Logger LOG =
      LoggerFactory.getLogger(ReconfigurableRegistry.class);

  public static void registReconfigurableProperty(String property,
      Reconfigurable reconfigurable) {
    synchronized (reconfMap) {
      reconfMap.put(property, reconfigurable);
    }
  }

  public static void registReconfigurableProperty(List<String> properties,
      Reconfigurable reconfigurable) {
    synchronized (reconfMap) {
      for (String p : properties) {
        reconfMap.put(p, reconfigurable);
      }
    }
  }

  public static void applyReconfigurablePropertyValue(String property, String value) {
    for (Reconfigurable c : getReconfigurables(property)) {
      try {
        c.reconfigureProperty(property, value);
      } catch (Exception e) {
        LOG.error("", e);
        // ignore and continue;
      }
    }
  }

  /**
   * Return modules interest in the property.
   * @param property
   * @return
   */
  public static List<Reconfigurable> getReconfigurables(String property) {
    synchronized (reconfMap) {
      return reconfMap.get(property);
    }
  }

  /**
   * Return configurable properties in system.
   * @return
   */
  public static Set<String> getAllReconfigurableProperties() {
    synchronized (reconfMap) {
      return reconfMap.keySet();
    }
  }
}
