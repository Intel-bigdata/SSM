package org.apache.hadoop.ssm;

public enum Property {
  ACCESSCOUNT, AGE;

  public static Property getPropertyType(String str) {
    if (str.equals("age"))
      return AGE;
    else if (str.equals("accessCount"))
      return ACCESSCOUNT;
    return null;
  }
}
