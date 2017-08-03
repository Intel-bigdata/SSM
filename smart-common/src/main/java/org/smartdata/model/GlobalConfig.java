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
  private String property_name;
  private String property_value;

  public GlobalConfig(int cid, String property_name, String property_value) {
    this.cid = cid;
    this.property_name = property_name;
    this.property_value = property_value;
  }

  public GlobalConfig() {
  }

  public void setCid(long cid) {
    this.cid = cid;
  }

  public void setProperty_name(String property_name) {
    this.property_name = property_name;
  }

  public void setProperty_value(String property_value) {
    this.property_value = property_value;
  }

  public long getCid() {
    return cid;
  }

  public String getProperty_name() {
    return property_name;
  }

  public String getProperty_value() {
    return property_value;
  }

  @Override
  public String toString() {
    return String.format("GlobalConfig{cid=%s, property_name=\'%s\', property_value=\'%s\'}",cid,property_name,property_value);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    GlobalConfig that = (GlobalConfig) o;

    if (cid != that.cid) return false;
    if (property_name != null ? !property_name.equals(that.property_name) : that.property_name != null) return false;
    return property_value != null ? property_value.equals(that.property_value) : that.property_value == null;
  }

  @Override
  public int hashCode() {
    int result = (int) (cid ^ (cid >>> 32));
    result = 31 * result + (property_name != null ? property_name.hashCode() : 0);
    result = 31 * result + (property_value != null ? property_value.hashCode() : 0);
    return result;
  }
}
