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
package org.smartdata.conf;

public class ReconfigureException extends Exception {
  private String reason;
  private String property;
  private String newVal;
  private String oldVal;

  public ReconfigureException(String reason) {
    super(reason);
    this.reason = reason;
    this.property = null;
    this.newVal = null;
    this.oldVal = null;
  }

  public ReconfigureException(String property, String newVal, String oldVal) {
    super(formatMessage(property, newVal, oldVal));
    this.property = property;
    this.newVal = newVal;
    this.oldVal = oldVal;
  }

  public ReconfigureException(String property, String newVal, String oldVal,
      Throwable cause) {
    super(formatMessage(property, newVal, oldVal), cause);
    this.property = property;
    this.newVal = newVal;
    this.oldVal = oldVal;
  }

  public String getReason() {
    return reason;
  }

  public String getProperty() {
    return property;
  }

  public String getNewValue() {
    return newVal;
  }

  public String getOldValue() {
    return oldVal;
  }

  private static String formatMessage(String property, String newVal, String oldVal) {
    return String.format("Failed to reconfig '%s' from '%s' to '%s'",
        property, oldVal, newVal);
  }
}
