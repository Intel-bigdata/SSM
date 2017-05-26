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
package org.smartdata.server.rule.parser;

/**
 * Created by root on 3/24/17.
 */
public enum OperatorType {
  NONE("none", false, ""),   // for error handling
  ADD("+", false),
  SUB("-", false),
  MUL("*", false),
  DIV("/", false),
  MOD("%", false),
  GT(">", true), // ">"
  GE(">=", true), // ">="
  LT("<", true), // "<"
  LE("<=", true), // "<="
  EQ("==", true), // "=="
  NE("!=", true, "<>"), // "!="
  MATCHES("matches", true, "LIKE"),
  AND("and", true, "AND"),
  OR("or", true, "OR"),
  NOT("not", true, "NOT"),
  ;

  private String name;
  private boolean isLogical;
  private String opInSql;

  public boolean isLogicalOperation() {
    return isLogical;
  }

  public static OperatorType fromString(String nameStr) {
    for (OperatorType v : values()) {
      if (v.name.equals(nameStr)) {
        return v;
      }
    }
    return NONE;
  }

  public String getOpInSql() {
    return opInSql == null ? name : opInSql;
  }

  private OperatorType(String name, boolean isLogical) {
    this.name = name;
    this.isLogical = isLogical;
  }

  private OperatorType(String name, boolean isLogical, String opInSql) {
    this.name = name;
    this.isLogical = isLogical;
    this.opInSql = opInSql;
  }
}
