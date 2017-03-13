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

import java.util.List;

/**
 * Property of SSM object.
 */
public class Property {
  public enum ParameterType {
    INT, LONG, STRING;
  }

  private int index;

  List<ParameterType> types;
  List<Object> parameters;  // value of each parameter

  public Property(int index) {
    this.index = index;
  }

  public Property(int index, List<ParameterType> types,
      List<Object> parameters) {
    this.index = index;
    this.types = types;
    this.parameters = parameters;
  }

  public int getPropertyIndex() {
    return index;
  }

  public List<ParameterType> getTypes() {
    return types;
  }

  public List<Object> getParameters() {
    return parameters;
  }
}
