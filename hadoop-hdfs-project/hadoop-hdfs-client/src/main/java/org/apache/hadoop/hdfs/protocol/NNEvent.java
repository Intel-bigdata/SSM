package org.apache.hadoop.hdfs.protocol;


public class NNEvent {
  public static final int EV_NULL = 0;
  public static final int EV_DELETE = 1;
  public static final int EV_RENAME = 2;

  private int eventType;
  private String[] args;

  public NNEvent(int type, String... args) {
    this.eventType = type;
    this.args = args;
  }

  public int getEventType() {
    return this.eventType;
  }

  public String[] getArgs() {
    return this.args;
  }
}
