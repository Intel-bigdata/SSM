package org.apache.hadoop.ssm;

public enum Action {
  CACHE, ARCHIVE, SSD;

  public static Action getActionType(String str) {
    if (str.equals("cache"))
      return CACHE;
    else if (str.equals("archive"))
      return ARCHIVE;
    else if (str.equals("ssd"))
      return SSD;
    return null;
  }
}
