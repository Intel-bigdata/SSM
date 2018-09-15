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

public class ErasureCodingPolicyInfo {
  private byte id;
  private String ecPolicyName;

  public ErasureCodingPolicyInfo(byte id, String ecPolicyName) {
    this.id = id;
    this.ecPolicyName = ecPolicyName;
  }

  public byte getID() {
    return id;
  }

  public void setID(byte id) {
    this.id = id;
  }

  public String getEcPolicyName() {
    return ecPolicyName;
  }

  public void setEcPolicyName(String policyName) {
    this.ecPolicyName = policyName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ErasureCodingPolicyInfo that = (ErasureCodingPolicyInfo) o;

    if (id != that.id) {
      return false;
    }
    return ecPolicyName != null ? ecPolicyName.equals(that.ecPolicyName) :
        that.ecPolicyName == null;
  }

  @Override
  public int hashCode() {
    int result = (int) id;
    result = 11 * result + (ecPolicyName != null ? ecPolicyName.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return String.format("StoragePolicy{id=%s, ecPolicyName=\'%s\'}", id, ecPolicyName);
  }
}
