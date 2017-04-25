package org.apache.hadoop.ssm.functions;

public interface CompareFunction<T> {
  Boolean compare(T left, T right);
}
