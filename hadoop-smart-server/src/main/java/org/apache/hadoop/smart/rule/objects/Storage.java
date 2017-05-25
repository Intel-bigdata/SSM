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
package org.apache.hadoop.smart.rule.objects;

import org.apache.hadoop.smart.rule.parser.ValueType;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Definition of rule object 'Storage'.
 */
public class Storage extends SmartObject {
  public static final Map<String, Property> properties;

  static {
    properties = new HashMap<>();
    properties.put("capacity", new Property("capacity", ValueType.LONG,
        Arrays.asList(ValueType.STRING), "storages", "capacity", true,
        "type = $0"));
    properties.put("free", new Property("free", ValueType.LONG,
        Arrays.asList(ValueType.STRING), "storages", "free", true,
        "type = $0 AND free"));
    properties.put("usedRatio", new Property("usedRatio", ValueType.LONG,
        Arrays.asList(ValueType.STRING), "storages", "free", true,
        "type = $0 AND (capacity - free) * 100.0 / capacity"));
  }

  public Storage() {
    super(ObjectType.STORAGE);
  }

  public Map<String, Property> getProperties() {
    return properties;
  }
}
