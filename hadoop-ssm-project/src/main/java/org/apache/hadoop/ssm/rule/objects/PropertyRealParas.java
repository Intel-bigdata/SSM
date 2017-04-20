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
package org.apache.hadoop.ssm.rule.objects;

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

  public boolean equals(PropertyRealParas paras) {
    return property.equals(paras.getProperty())
        && equalsValue(paras.getValues());
  }

  public boolean equalsValue(List<Object> v) {
    if (values == null && v == null) {
      return true;
    }

    if (values == null || v == null) {
      return false;
    }

    if (values.size() != v.size()) {
      return false;
    }

    for (int i = 0; i < v.size(); i++) {
      if (v.get(i).getClass() != values.get(i).getClass()) {
        return false;
      }

      if (!v.get(i).equals(values.get(i))) {
        return false;
      }
    }
    return true;
  }
}
