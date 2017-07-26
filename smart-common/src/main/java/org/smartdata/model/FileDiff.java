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

public class FileDiff {
  private long diffId;
  private String parameters;
  private FileDiffType diffType;
  private boolean applied;
  private long create_time;

  public long getDiffId() {
    return diffId;
  }

  public void setDiffId(long diffId) {
    this.diffId = diffId;
  }

  public String getParameters() {
    return parameters;
  }

  public void setParameters(String parameters) {
    this.parameters = parameters;
  }

  public FileDiffType getDiffType() {
    return diffType;
  }

  public void setDiffType(FileDiffType diffType) {
    this.diffType = diffType;
  }

  public boolean isApplied() {
    return applied;
  }

  public void setApplied(boolean applied) {
    this.applied = applied;
  }

  public long getCreate_time() {
    return create_time;
  }

  public void setCreate_time(long create_time) {
    this.create_time = create_time;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof FileDiff)) {
      return false;
    }

    FileDiff fileDiff = (FileDiff) o;

    if (getDiffId() != fileDiff.getDiffId()) {
      return false;
    }
    if (isApplied() != fileDiff.isApplied()) {
      return false;
    }
    if (getCreate_time() != fileDiff.getCreate_time()) {
      return false;
    }
    if (!getParameters().equals(fileDiff.getParameters())) {
      return false;
    }
    return getDiffType() == fileDiff.getDiffType();
  }

  @Override
  public int hashCode() {
    int result = (int) (getDiffId() ^ (getDiffId() >>> 32));
    result = 31 * result + getParameters().hashCode();
    result = 31 * result + getDiffType().hashCode();
    result = 31 * result + (isApplied() ? 1 : 0);
    result = 31 * result + (int) (getCreate_time() ^ (getCreate_time() >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return String.format("FileDiff{diffId=%s, parameters=%s, " +
            "diffType=%s, applied=%s, create_time=%s}", diffId, parameters,
        diffType, applied, create_time);
  }
}
