package org.apache.hadoop.ssm.functions;

public class BinaryCondition<T> {
  private final CompareFunction<T> function;
  private final T threshold;

  public BinaryCondition(CompareFunction<T> function, T threshold) {
    this.function = function;
    this.threshold = threshold;
  }

  public Boolean pass(T value) {
    return this.function.compare(value, threshold);
  }
}
