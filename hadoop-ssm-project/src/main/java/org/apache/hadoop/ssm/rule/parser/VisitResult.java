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
package org.apache.hadoop.ssm.rule.parser;

import java.io.IOException;

/**
 * Represent a value or a identifier in the parser tree.
 */
public class VisitResult {

  private ValueType type;
  private Object value;

  public VisitResult() {
    this.type = ValueType.ERROR;
  }

  public VisitResult(ValueType type) {
    this.type = type;
  }

  public void setValue(Object value) {
    this.value = value;
  }

  public VisitResult(ValueType type, Object value) {
    this.type = type;
    this.value = value;
  }

  public ValueType getValueType() {
    return type;
  }

  public Object getValue() {
    return value;
  }

  public VisitResult eval(OperatorType type, VisitResult dst) throws IOException {
    VisitResult r;
    switch (type) {
      case GT:
        r = new VisitResult(ValueType.BOOLEAN, this.compareTo(dst) > 0);
        break;
      case GE:
        r = new VisitResult(ValueType.BOOLEAN, this.compareTo(dst) >= 0);
        break;
      case EQ:
        r = new VisitResult(ValueType.BOOLEAN, this.compareTo(dst) == 0);
        break;
      case LT:
        r = new VisitResult(ValueType.BOOLEAN, this.compareTo(dst) < 0);
        break;
      case LE:
        r = new VisitResult(ValueType.BOOLEAN, this.compareTo(dst) <= 0);
        break;
      case NE:
        r = new VisitResult(ValueType.BOOLEAN, this.compareTo(dst) != 0);
        break;
      case AND:
        r = new VisitResult(ValueType.BOOLEAN, toBoolean() && dst.toBoolean());
        break;
      case OR:
        r = new VisitResult(ValueType.BOOLEAN, toBoolean() || dst.toBoolean());
        break;
      case NOT:
        r = new VisitResult(ValueType.BOOLEAN, !toBoolean());
        break;
      // TODO:
      default:
        throw new IOException("Unknown type");
    }
    return r;
  }

  private boolean toBoolean() throws IOException {
    if (type != ValueType.BOOLEAN) {
      throw new IOException("Type must be boolean: " + this);
    }
    return (Boolean)value;
  }

  private int compareTo(VisitResult dst) throws IOException {
    if (dst.getValueType() != this.type) {
      throw new IOException("Type miss match for compare: [1] "
          + this + " [2] " + dst);
    }

    switch (type) {
      case LONG:
      case TIMEINTVAL:
      case TIMEPOINT:
        return (int)((Long)value - (Long)(dst.getValue()));
      case STRING:
        return ((String)value).equals((String)(dst.getValue())) ? 0 : 1;
      default:
        throw new IOException("Invalid type for compare: [1] "
            + this + " [2] " + dst);
    }
  }



}
