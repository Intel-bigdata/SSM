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
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    BackUpInfo that = (BackUpInfo) o;

    if (rid != that.rid) return false;
    if (period != that.period) return false;
    if (src != null ? !src.equals(that.src) : that.src != null) return false;
    return dest != null ? dest.equals(that.dest) : that.dest == null;
  }

  @Override
  public int hashCode() {
    int result = (int) (rid ^ (rid >>> 32));
    result = 31 * result + (src != null ? src.hashCode() : 0);
    result = 31 * result + (dest != null ? dest.hashCode() : 0);
    result = 31 * result + (int) (period ^ (period >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return String.format("BackUpInfo{rid=%s, src\'%s\', dest=\'%s\', period=%s}", rid, src, dest, period);
  }
}
