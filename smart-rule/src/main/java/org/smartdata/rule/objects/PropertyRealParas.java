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

/**
 * Log parameters for a property.
 */
public class PropertyRealParas {
  private Property property;
  private List<Object> values;

  public PropertyRealParas(Property p, List<Object> values) {
    this.property = p;
    this.values = values;
  }

  public Property getProperty() {
    return property;
  }

  public List<Object> getValues() {
    return values;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    PropertyRealParas that = (PropertyRealParas) o;

    if (property != null ? !property.equals(that.property) : that.property != null) return false;
    return values != null ? values.equals(that.values) : that.values == null;
  }

  @Override
  public int hashCode() {
    int result = property != null ? property.hashCode() : 0;
    result = 31 * result + (values != null ? values.hashCode() : 0);
    return result;
  }

  public String formatParameters() {
    return property.formatParameters(values);
  }

  public String instId() {
    return property.instId(values);
  }
}
