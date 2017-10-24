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
package org.smartdata.rule.objects;

import java.util.List;
import java.util.Map;

/**
 * Acts as base of SSM objects.
 */
public abstract class SmartObject {

  private ObjectType type;

  public SmartObject(ObjectType type) {
    this.type = type;
  }

  public ObjectType getType() {
    return type;
  }

  private List<Property> requiredProperties;

  public static SmartObject getInstance(String typeName) {
    // TODO: create through class name
    switch (typeName) {
      case "file":
        return new FileObject();
      case "storage":
        return new StorageObject();
      default:
        return null;
    }
  }

  /**
   * The following PROPERTIES of this Object are required.
   * @param properties
   */
  public void setRequiredProperties(List<Property> properties) {
    requiredProperties = properties;
  }

  public List<Property> getPropertyRequired() {
    return requiredProperties;
  }

  public boolean containsProperty(String propertyName) {
    return getProperties().get(propertyName) != null;
  }

  public Property getProperty(String propertyName) {
    return getProperties().get(propertyName);
  }

  public abstract Map<String, Property> getProperties();
}
