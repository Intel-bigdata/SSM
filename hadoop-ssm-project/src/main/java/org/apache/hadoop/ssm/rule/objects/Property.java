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

import org.apache.hadoop.ssm.rule.parser.ValueType;

import java.util.List;

/**
 * Property of SSM object.
 */
public class Property {

  private ValueType retType;
  private List<ValueType> paramsTypes;

  private String tableName;
  private String tableItemName;
  private String formatTemplate;
  private boolean isGlobal;

  public Property(ValueType retType, List<ValueType> paramsTypes,
       String tableName, String tableItemName, boolean isGlobal) {
    this.retType = retType;
    this.paramsTypes = paramsTypes;
    this.tableName = tableName;
    this.tableItemName = tableItemName;
    this.isGlobal = isGlobal;
  }

  // TODO: re-arch to couple paramsTypes and formatTemplate
  public Property(ValueType retType, List<ValueType> paramsTypes,
      String tableName, String tableItemName, boolean isGlobal,
      String formatTemplate) {
    this.retType = retType;
    this.paramsTypes = paramsTypes;
    this.tableName = tableName;
    this.tableItemName = tableItemName;
    this.formatTemplate = formatTemplate;
    this.isGlobal = isGlobal;
  }

  public ValueType getValueType() {
    return retType;
  }

  public List<ValueType> getParamsTypes() {
    return paramsTypes;
  }

  public String getTableName() {
    return tableName;
  }

  public String getTableItemName() {
    return tableItemName;
  }

  public boolean isGlobal() {
    return isGlobal;
  }

  public boolean hasParameters() {
    return paramsTypes != null;
  }

  public boolean equals(Property p) {
    if (p == null) {
      return false;
    }
    // null
    boolean others = retType == p.getValueType()
        && tableName.equals(p.getTableName())
        && tableItemName.equals(p.getTableItemName())
        && isGlobal == p.isGlobal();

    if (!others) {
      return false;
    }

    if (paramsTypes == null && p.getParamsTypes() == null) {
      return true;
    }

    if (paramsTypes == null || p.getParamsTypes() == null) {
      return false;
    }

    if (paramsTypes.size() != p.getParamsTypes().size()) {
      return false;
    }

    for (int i = 0; i < paramsTypes.size(); i++) {
      if (!paramsTypes.get(i).equals(p.getParamsTypes().get(i))) {
        return false;
      }
    }

    return true;
  }

  public String formatParameters(List<Object> values) {
    if (formatTemplate == null) {
      return tableItemName;
    }

    String ret = formatTemplate;

    // TODO: need more checks to ensure replace correctly
    for (int i = 0; i < values.size(); i++) {
      if (ret.contains("$" + i)) {
        String v;
        switch (paramsTypes.get(i)) {
          case TIMEINTVAL:
          case LONG:
            v = "" + ((Long) values.get(i));
            break;
          case STRING:
            v = "'" + ((String) values.get(i)) + "'";
            break;
          default:
            v = null;  // TODO: throw exception
        }
        if (v != null) {
          ret = ret.replaceAll("\\$" + i, v);
        }
      }
    }
    return ret;
  }
}
