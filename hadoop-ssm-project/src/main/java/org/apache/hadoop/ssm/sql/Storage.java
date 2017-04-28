package org.apache.hadoop.ssm.sql;

public final class Storage {
  private final String type;
  private final long capacity;
  private final long free;
  public Storage(String type, long capacity, long free) {
    this.type = type;
    this.capacity = capacity;
    this.free = free;
  }
  public String getType() { return type; }
  public long getCapacity() { return capacity; }
  public long getFree() { return free; }
}
