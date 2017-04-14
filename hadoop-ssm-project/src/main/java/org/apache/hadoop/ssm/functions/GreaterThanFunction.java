package org.apache.hadoop.ssm.functions;

public class GreaterThanFunction<T extends Comparable<T>> implements CompareFunction<T> {
  @Override
  public Boolean compare(T left, T right) {
    return left.compareTo(right) > 0;
  }
}
