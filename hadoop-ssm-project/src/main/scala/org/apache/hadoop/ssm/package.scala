package org.apache.hadoop

package object ssm {
  type Condition[T] = (T) => Boolean
}
