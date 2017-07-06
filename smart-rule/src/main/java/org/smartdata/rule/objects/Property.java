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

import org.smartdata.rule.parser.ValueType;

import java.util.List;

/**
 * Property of SSM object.
 */
public class Property {
  private String propertyName;
  private ValueType retType;
  private List<ValueType> paramsTypes;

  private String tableName;
  private String tableItemName;
  private String formatTemplate;
  private boolean isGlobal;

  public Property(String propertyName, ValueType retType, List<ValueType> paramsTypes,
       String tableName, String tableItemName, boolean isGlobal) {
    this.propertyName = propertyName;
    this.retType = retType;
    this.paramsTypes = paramsTypes;
    this.tableName = tableName;
    this.tableItemName = tableItemName;
    this.isGlobal = isGlobal;
  }

  // TODO: re-arch to couple paramsTypes and formatTemplate
  public Property(String propertyName, ValueType retType,
      List<ValueType> paramsTypes, String tableName,
      String tableItemName, boolean isGlobal,
      String formatTemplate) {
    this.propertyName = propertyName;
    this.retType = retType;
    this.paramsTypes = paramsTypes;
    this.tableName = tableName;
    this.tableItemName = tableItemName;
    this.formatTemplate = formatTemplate;
    this.isGlobal = isGlobal;
  }

  public String getPropertyName() {
    return propertyName;
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

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Property property = (Property) o;

    if (isGlobal != property.isGlobal) return false;
    if (propertyName != null ? !propertyName.equals(property.propertyName) : property.propertyName != null)
      return false;
    if (retType != property.retType) return false;
    if (paramsTypes != null ? !paramsTypes.equals(property.paramsTypes) : property.paramsTypes != null) return false;
    if (tableName != null ? !tableName.equals(property.tableName) : property.tableName != null) return false;
    if (tableItemName != null ? !tableItemName.equals(property.tableItemName) : property.tableItemName != null)
      return false;
    return formatTemplate != null ? formatTemplate.equals(property.formatTemplate) : property.formatTemplate == null;
  }

  @Override
  public int hashCode() {
    int result = propertyName != null ? propertyName.hashCode() : 0;
    result = 31 * result + (retType != null ? retType.hashCode() : 0);
    result = 31 * result + (paramsTypes != null ? paramsTypes.hashCode() : 0);
    result = 31 * result + (tableName != null ? tableName.hashCode() : 0);
    result = 31 * result + (tableItemName != null ? tableItemName.hashCode() : 0);
    result = 31 * result + (formatTemplate != null ? formatTemplate.hashCode() : 0);
    result = 31 * result + (isGlobal ? 1 : 0);
    return result;
  }

  public String instId(List<Object> values) {
    if (getParamsTypes() == null) {
      return propertyName;
    }
    String ret = propertyName;
    assert(values.size() == getParamsTypes().size());
    for (int i = 0; i < values.size(); i++) {
      switch (getValueType()) {
        case TIMEINTVAL:
        case LONG:
          ret += "_" + ((Long) values.get(i));
          break;
        case STRING:
          ret += "_" + ((String) values.get(i)).replaceAll("[\t -\"']+", "_");
          break;
        default:
           assert (false);  // TODO: throw exception
      }
    }
    return ret;
  }

  public String formatParameters(List<Object> values) {
    if (formatTemplate == null) {
      return tableItemName;
    }

    if (values == null) {
      return formatTemplate;
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
