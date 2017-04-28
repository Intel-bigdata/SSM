package org.apache.hadoop.ssm.sql;

public final class StorageCapacity {
  private final String type;
  private final Long capacity;
  private final Long free;
  public StorageCapacity(String type, Long capacity, Long free) {
    this.type = type;
    this.capacity = capacity;
    this.free = free;
  }
  public String getType() { return type; }
  public Long getCapacity() { return capacity; }
  public Long getFree() { return free; }

}
