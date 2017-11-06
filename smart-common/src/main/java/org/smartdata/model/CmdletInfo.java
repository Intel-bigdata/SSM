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

import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class CmdletInfo {
  private long cid;
  private long rid;
  private List<Long> aids;
  private CmdletState state;
  private String parameters;
  private long generateTime;
  private long stateChangedTime;

  public CmdletInfo() {
  }

  public CmdletInfo(long cid, long rid, CmdletState state,
      String parameters, long generateTime, long stateChangedTime) {
    this.cid = cid;
    this.rid = rid;
    this.state = state;
    this.parameters = parameters;
    this.generateTime = generateTime;
    this.stateChangedTime = stateChangedTime;
    this.aids = new ArrayList<>();
  }

  public CmdletInfo(long cid, long rid, List<Long> aids, CmdletState state,
      String parameters, long generateTime, long stateChangedTime) {
    this(cid, rid, state, parameters, generateTime, stateChangedTime);
    this.aids = aids;
  }

  @Override
  public String toString() {
    return String.format("CmdletId -> [ %s ] {rid = %d, aids = %s, genTime = %d, "
            + "stateChangedTime = %d, state = %s, params = %s}",
        cid, rid, StringUtils.join(getAidsString(), ","),
        generateTime, stateChangedTime, state,
        parameters);
  }

  public void addAction(long aid) {
    aids.add(aid);
  }

  public List<Long> getAids() {
    return aids;
  }

  public List<String> getAidsString() {
    List<String> ret = new ArrayList<>();
    for (Long aid : aids) {
      ret.add(String.valueOf(aid));
    }
    return ret;
  }

  public long getCid() {
    return cid;
  }

  public void setCid(long cid) {
    this.cid = cid;
  }

  public long getRid() {
    return rid;
  }

  public void setRid(long rid) {
    this.rid = rid;
  }

  public void setAids(List<Long> aids) {
    this.aids = aids;
  }

  public CmdletState getState() {
    return state;
  }

  public void setState(CmdletState state) {
    this.state = state;
  }

  public void updateState(CmdletState state) {
    if (this.state != state) {
      this.state = state;
      this.stateChangedTime = System.currentTimeMillis();
    }
  }

  public String getParameters() {
    return parameters;
  }

  public void setParameters(String parameters) {
    this.parameters = parameters;
  }

  public long getGenerateTime() {
    return generateTime;
  }

  public void setGenerateTime(long generateTime) {
    this.generateTime = generateTime;
  }

  public long getStateChangedTime() {
    return stateChangedTime;
  }

  public void setStateChangedTime(long stateChangedTime) {
    this.stateChangedTime = stateChangedTime;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CmdletInfo that = (CmdletInfo) o;
    return cid == that.cid
        && rid == that.rid
        && generateTime == that.generateTime
        && stateChangedTime == that.stateChangedTime
        && Objects.equals(aids, that.aids)
        && state == that.state
        && Objects.equals(parameters, that.parameters);
  }

  @Override
  public int hashCode() {
    return Objects.hash(cid, rid, aids, state, parameters, generateTime, stateChangedTime);
  }

  public static Builder newBuilder() {
    return Builder.create();
  }

  public static class Builder {
    private long cid;
    private long rid;
    private List<Long> aids;
    private CmdletState state;
    private String parameters;
    private long generateTime;
    private long stateChangedTime;

    public static Builder create() {
      return new Builder();
    }

    public Builder setCid(long cid) {
      this.cid = cid;
      return this;
    }

    public Builder setRid(long rid) {
      this.rid = rid;
      return this;
    }

    public Builder setAids(List<Long> aids) {
      this.aids = aids;
      return this;
    }


    public Builder setState(CmdletState state) {
      this.state = state;
      return this;
    }

    public Builder setParameters(String parameters) {
      this.parameters = parameters;
      return this;
    }

    public Builder setGenerateTime(long generateTime) {
      this.generateTime = generateTime;
      return this;
    }

    public Builder setStateChangedTime(long stateChangedTime) {
      this.stateChangedTime = stateChangedTime;
      return this;
    }

    public CmdletInfo build() {
      return new CmdletInfo(cid, rid, aids, state, parameters,
          generateTime, stateChangedTime);
    }
  }
}
