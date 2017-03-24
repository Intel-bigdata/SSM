package org.apache.hadoop.ssm.rule.parser;

import java.io.IOException;

/**
 * Created by root on 3/23/17.
 */
public class VisitResult extends TreeNode {

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

  }


  boolean greaterThan(VisitResult dst) throws IOException {
    return this.compareTo(dst) > 0;
  }

  boolean greaterEqualThan(VisitResult dst) throws IOException {
    return this.compareTo(dst) >= 0;
  }

  boolean equal(VisitResult dst) throws IOException {
    return this.compareTo(dst) == 0;
  }

  boolean littlerThan(VisitResult dst) throws IOException {
    return this.compareTo(dst) < 0;
  }

  boolean litterEqualThan(VisitResult dst) throws IOException {
    return this.compareTo(dst) <= 0;
  }
  boolean notEqual(VisitResult dst) throws IOException {
    return this.compareTo(dst) != 0;
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


  public boolean isLeaf() {
    return true;
  }
}
