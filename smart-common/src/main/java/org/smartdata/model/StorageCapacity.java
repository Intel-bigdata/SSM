/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.smartdata.model;

public final class StorageCapacity {
  private String type;
  private Long timeStamp;
  private Long capacity;
  private Long free;

  public StorageCapacity(String type, Long timeStamp, Long capacity, Long free) {
    this.type = type;
    this.timeStamp = timeStamp;
    this.capacity = capacity;
    this.free = free;
  }

  public StorageCapacity(String type, Long capacity, Long free) {
    this.type = type;
    this.capacity = capacity;
    this.free = free;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getType() {
    return type;
  }

  public Long getCapacity() {
    return capacity;
  }

  public Long getTimeStamp() {
    return timeStamp;
  }

  public void setTimeStamp(Long timeStamp) {
    this.timeStamp = timeStamp;
  }

  public void addCapacity(long v) {
    capacity += v;
  }

  public Long getFree() {
    return free;
  }

  public Long getUsed() {
    return capacity - free;
  }

  public void addFree(long v) {
    free += v;
  }

  @Override
  public boolean equals(Object o) { // timeStamp not considered
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    StorageCapacity that = (StorageCapacity) o;

    if (type != null ? !type.equals(that.type) : that.type != null) {
      return false;
    }
    if (capacity != null ? !capacity.equals(that.capacity) : that.capacity != null) {
      return false;
    }
    return free != null ? free.equals(that.free) : that.free == null;
  }

  @Override
  public int hashCode() {
    int result = type != null ? type.hashCode() : 0;
    result = 31 * result + (capacity != null ? capacity.hashCode() : 0);
    result = 31 * result + (free != null ? free.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return String.format(
        "StorageCapacity{type=\'%s\', capacity=%s, free=%s}", type, capacity, free);
  }
}
