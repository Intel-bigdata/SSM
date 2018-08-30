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

/**
 * Compression/Codec enum.
 */
public enum CompressionType {
  /**
   * uncompressed/raw data.
   */
  raw(0),
  /**
   * snappy.
   */
  snappy(1),
  /**
   * Lz4.
   */
  Lz4(2),
  /**
   * Bzip2, splitable.
   */
  Bzip2(3),
  /**
   * Zlib.
   */
  Zlib(4);

  private final int value;

  CompressionType(int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }

  /**
   * Return CompressionType from a given value (int).
   * @param value (int)
   * @return CompressionType of this value
   */
  public static CompressionType fromValue(int value) {
    for (CompressionType t : values()) {
      if (t.getValue() == value) {
        return t;
      }
    }
    return null;
  }

  /**
   * Return CompressionType from a given String.
   * @param name String of a codec
   * @return CompressionType of this String
   */
  public static CompressionType fromName(String name) {
    for (CompressionType t : values()) {
      if (t.toString().equalsIgnoreCase(name)) {
        return t;
      }
    }
    return null;
  }
}
