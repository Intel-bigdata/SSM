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

public class StoragePolicy {
  private byte sid;
  private String policyName;

  public StoragePolicy(byte sid, String policyName) {
    this.sid = sid;
    this.policyName = policyName;
  }

  public byte getSid() {
    return sid;
  }

  public void setSid(byte sid) {
    this.sid = sid;
  }

  public String getPolicyName() {
    return policyName;
  }

  public void setPolicyName(String policyName) {
    this.policyName = policyName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    StoragePolicy that = (StoragePolicy) o;

    if (sid != that.sid) {
      return false;
    }
    return policyName != null ? policyName.equals(that.policyName) :
        that.policyName == null;
  }

  @Override
  public int hashCode() {
    int result = (int) sid;
    result = 31 * result + (policyName != null ? policyName.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "StoragePolicy{" +
        "sid=" + sid +
        ", policyName='" + policyName + '\'' +
        '}';
  }
}
