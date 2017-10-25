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
package org.smartdata.server.engine.data;


import java.util.HashMap;
import java.util.Map;

/**
 * Abstract of rule execution environment.
 */
public class ExecutionContext {
  public static final String RULE_ID = "RuleId";
  private Map<String, Object> envVariables = new HashMap<>();

  public long getRuleId() {
    return getLong(RULE_ID);
  }

  public void setRuleId(long ruleId) {
    envVariables.put(RULE_ID, ruleId);
  }

  public void setProperties(Map<String, Object> properties) {
    if (properties == null) {
      envVariables.clear();
    } else {
      envVariables = properties;
    }
  }

  public void setProperty(String property, Object value) {
    envVariables.put(property, value);
  }

  public String getString(String property) {
    Object val = envVariables.get(property);
    if (val == null) {
      return null;
    }
    return val.toString();
  }

  public Long getLong(String property) {
    Object val = envVariables.get(property);
    if (val == null) {
      return null;
    }
    if (val instanceof Integer) {
      return Long.valueOf((Integer) val);
    } else if (val instanceof Long) {
      return (Long) val;
    } else if (val instanceof String) {
      try {
        return Long.parseLong((String) val);
      } catch (NumberFormatException e) {
        return null;
      }
    }
    return null;
  }
}
