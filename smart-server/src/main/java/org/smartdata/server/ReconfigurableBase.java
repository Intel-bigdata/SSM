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
import org.smartdata.conf.ReconfigureException;

import java.util.Collection;
import java.util.List;
import java.util.Set;

public abstract class ReconfigurableBase {
  private static ListMultimap<String, ReconfigurableBase> reconfMap =
      ArrayListMultimap.create();

  public ReconfigurableBase() {
    Collection<String> properties = getReconfigurableProperties();
    if (properties != null) {
      for (String p : properties) {
        reconfMap.put(p, this);
      }
    }
  }

  /**
   * Called when the property's value is reconfigured.
   * @param property
   * @param newVal
   * @throws ReconfigureException
   */
  protected abstract void reconfigureProperty(String property, String newVal)
      throws ReconfigureException;

  /**
   * Return the reconfigurable properties that supported.
   * @return
   */
  public abstract Collection<String> getReconfigurableProperties();

  /**
   * Return modules interest in the property.
   * @param property
   * @return
   */
  public static List<ReconfigurableBase> getReconfigurables(String property) {
    return reconfMap.get(property);
  }

  /**
   * Return configurable properties in system.
   * @return
   */
  public static Set<String> getAllReconfigurableProperties() {
    return reconfMap.keySet();
  }
}
