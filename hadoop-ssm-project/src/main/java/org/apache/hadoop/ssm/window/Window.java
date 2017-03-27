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
package org.apache.hadoop.ssm.window;

public class Window {
  private final Long start;
  private final Long end;

  public Window(Long start, Long end) {
    this.start = start;
    this.end = end;
  }

  public Long getLength() {
    return this.end - this.start;
  }

  public Boolean intersects(Window other) {
    return this.start <= other.end && this.end >= other.start;
  }

  public Boolean include(Window other) {
    return (this.end >= other.end) && (this.start <= other.start);
  }

  public Long getStart() {
    return start;
  }

  public Long getEnd() {
    return end;
  }
}
