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

import java.util.Arrays;
import java.util.Objects;

public class XAttribute {
  private final String nameSpace;
  private final String name;
  private final byte[] value;

  public XAttribute(String nameSpace, String name, byte[] value) {
    this.nameSpace = nameSpace;
    this.name = name;
    this.value = value;
  }

  public String getNameSpace() {
    return nameSpace;
  }

  public String getName() {
    return name;
  }

  public byte[] getValue() {
    return value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    XAttribute that = (XAttribute) o;
    return Objects.equals(nameSpace, that.nameSpace)
        && Objects.equals(name, that.name)
        && Arrays.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(nameSpace, name, value);
  }
}
