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
package org.smartdata.rule.parser;

import org.smartdata.rule.objects.PropertyRealParas;

import java.io.IOException;

/** Represent a value or a identifier in the parser tree. */
public class VisitResult {

  private ValueType type;
  private Object value;
  private PropertyRealParas realParas;

  public void setValue(Object value) {
    this.value = value;
  }

  public VisitResult(ValueType type, Object value) {
    this.type = type;
    this.value = value;
  }

  public VisitResult(ValueType type, Object value, PropertyRealParas realParas) {
    this.type = type;
    this.value = value;
    this.realParas = realParas;
  }

  public PropertyRealParas getRealParas() {
    return realParas;
  }

  public ValueType getValueType() {
    return type;
  }

  public Object getValue() {
    // TODO: handle identify issu
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
      case ADD:
      case SUB:
      case MUL:
      case DIV:
      case MOD:
        r = generateCalc(type, dst);
        break;
        // TODO:
      default:
        throw new IOException("Unknown type");
    }
    return r;
  }

  private VisitResult generateCalc(OperatorType opType, VisitResult dst) {
    ValueType retType = null;
    Object retValue = null;
    boolean haveTimePoint =
        getValueType() == ValueType.TIMEPOINT
            || (dst != null && dst.getValueType() == ValueType.TIMEPOINT);

    if (haveTimePoint) {
      if (opType == OperatorType.ADD) {
        retType = ValueType.TIMEPOINT;
      } else if (opType == OperatorType.SUB) {
        retType = ValueType.TIMEINTVAL;
      }
    }

    if (retType == null) {
      retType = getValueType();
    }

    switch (type) {
      case STRING:
        switch (opType) {
          case ADD:
            retValue = (String) getValue() + (String) dst.getValue();
        }
        break;

      default:
        Long r1 = (Long) getValue();
        Long r2 = (Long) dst.getValue();
        switch (opType) {
          case ADD:
            retValue = r1 + r2;
            break;
          case SUB:
            retValue = r1 - r2;
            break;
          case MUL:
            retValue = r1 * r2;
            break;
          case DIV:
            retValue = r1 / r2;
            break;
          case MOD:
            retValue = r1 % r2;
            break;
        }
    }
    return new VisitResult(retType, retValue);
  }

  private boolean toBoolean() throws IOException {
    if (type != ValueType.BOOLEAN) {
      throw new IOException("Type must be boolean: " + this);
    }
    return (Boolean) getValue();
  }

  private int compareTo(VisitResult dst) throws IOException {
    if (dst.getValueType() != this.type) {
      throw new IOException("Type miss match for compare: [1] " + this + " [2] " + dst);
    }

    switch (type) {
      case LONG:
      case TIMEINTVAL:
      case TIMEPOINT:
        return (int) ((Long) getValue() - (Long) (dst.getValue()));
      case STRING:
        return ((String) getValue()).equals((String) (dst.getValue())) ? 0 : 1;
      default:
        throw new IOException("Invalid type for compare: [1] " + this + " [2] " + dst);
    }
  }

  public boolean isConst() {
    return type != ValueType.ERROR && getValue() != null;
  }
}
