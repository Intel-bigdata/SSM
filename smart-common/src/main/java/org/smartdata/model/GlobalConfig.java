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
package org.smartdata.model;

public class GlobalConfig {
  private long cid;
  private String propertyName;
  private String propertyValue;

  public GlobalConfig(int cid, String propertyName, String propertyValue) {
    this.cid = cid;
    this.propertyName = propertyName;
    this.propertyValue = propertyValue;
  }

  public GlobalConfig() {
  }

  public void setCid(long cid) {
    this.cid = cid;
  }

  public void setPropertyName(String propertyName) {
    this.propertyName = propertyName;
  }

  public void setPropertyValue(String propertyValue) {
    this.propertyValue = propertyValue;
  }

  public long getCid() {
    return cid;
  }

  public String getPropertyName() {
    return propertyName;
  }

  public String getPropertyValue() {
    return propertyValue;
  }

  @Override
  public String toString() {
    return String.format("GlobalConfig{cid=%s, propertyName=\'%s\', propertyValue=\'%s\'}",cid,propertyName,propertyValue);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    GlobalConfig that = (GlobalConfig) o;

    if (cid != that.cid) return false;
    if (propertyName != null ? !propertyName.equals(that.propertyName) : that.propertyName != null) return false;
    return propertyValue != null ? propertyValue.equals(that.propertyValue) : that.propertyValue == null;
  }

  @Override
  public int hashCode() {
    int result = (int) (cid ^ (cid >>> 32));
    result = 31 * result + (propertyName != null ? propertyName.hashCode() : 0);
    result = 31 * result + (propertyValue != null ? propertyValue.hashCode() : 0);
    return result;
  }
}
