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


public enum FileDiffType {
  CREATE(0),
  DELETE(1),
  RENAME(2),
  APPEND(3),
  METADATA(4),
  BASESYNC(5);

  private int value;

  FileDiffType(int value) {
    this.value = value;
  }

  public static FileDiffType fromValue(int value) {
    for (FileDiffType r : values()) {
      if (value == r.getValue()) {
        return r;
      }
    }
    return null;
  }

  public int getValue() {
    return value;
  }

  @Override
  public String toString() {
    return String.format("FileDiffType{value=%s} %s", value, super.toString());
  }

}
