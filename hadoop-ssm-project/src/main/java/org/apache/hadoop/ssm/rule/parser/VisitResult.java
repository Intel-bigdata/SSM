package org.apache.hadoop.ssm.rule.parser;

/**
 * Created by root on 3/23/17.
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
}
