/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.smartdata.model;

import java.util.Objects;

public class BackUpInfo {
  private long rid;
  private String src;
  private String dest;
  private long period; // in milli-seconds


  public BackUpInfo(long rid, String src, String dest, long period) {
    this.rid = rid;
    this.src = src;
    this.dest = dest;
    this.period = period;
  }

  public BackUpInfo() {
  }

  public long getRid() {
    return rid;
  }

  public void setRid(long rid) {
    this.rid = rid;
  }

  public String getSrc() {
    return src;
  }

  public void setSrc(String src) {
    this.src = src;
  }

  public String getDest() {
    return dest;
  }

  public void setDest(String dest) {
    this.dest = dest;
  }

  public long getPeriod() {
    return period;
  }

  public void setPeriod(long period) {
    this.period = period;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BackUpInfo that = (BackUpInfo) o;
    return rid == that.rid
        && period == that.period
        && Objects.equals(src, that.src)
        && Objects.equals(dest, that.dest);
  }

  @Override
  public int hashCode() {
    return Objects.hash(rid, src, dest, period);
  }

  @Override
  public String toString() {
    return String.format(
        "BackUpInfo{rid=%s, src\'%s\', dest=\'%s\', period=%s}", rid, src, dest, period);
  }
}
