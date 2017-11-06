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

/**
 * Provide info to action user.
 */
public class ActionDescriptor {
  private String actionName;
  private String displayName; // if not provided, use actionName instead
  private String usage;     // Info about usage/arguments if any
  private String comment;   // Provide detailed info if any

  public ActionDescriptor(String actionName, String displayName,
      String usage, String comment) {
    this.actionName = actionName;
    this.displayName = displayName;
    this.usage = usage;
    this.comment = comment;
  }

  public static Builder newBuilder() {
    return Builder.create();
  }

  public String getActionName() {
    return actionName;
  }

  public void setActionName(String actionName) {
    this.actionName = actionName;
  }

  public String getDisplayName() {
    return displayName;
  }

  public void setDisplayName(String displayName) {
    this.displayName = displayName;
  }

  public String getUsage() {
    return usage;
  }

  public void setUsage(String usage) {
    this.usage = usage;
  }

  public String getComment() {
    return comment;
  }

  public void setComment(String comment) {
    this.comment = comment;
  }

  @Override
  public String toString() {
    return String.format(
        "ActionDescriptor{actionName=\'%s\', displayName=\'%s\', "
            + "displayName=\'%s\', usage=\'%s\', comment=\'%s\', comment=\'%s\'}",
        actionName, displayName, usage, comment);
  }

  public static class Builder {
    private String actionName;
    private String displayName; // if not provided, use actionName instead
    private String usage;     // Info about usage/arguments if any
    private String comment;   // Provide detailed info if any

    public Builder setActionName(String actionName) {
      this.actionName = actionName;
      return this;
    }

    public Builder setDisplayName(String displayName) {
      this.displayName = displayName;
      return this;
    }

    public Builder setUsage(String usage) {
      this.usage = usage;
      return this;
    }

    public Builder setComment(String comment) {
      this.comment = comment;
      return this;
    }

    public static Builder create() {
      return new Builder();
    }

    public ActionDescriptor build() {
      return new ActionDescriptor(actionName, displayName, usage, comment);
    }

    @Override
    public String toString() {
      return String.format(
          "Builder{actionName=\'%s\', displayName=\'%s\', usage=\'%s\', comment=\'%s\'}",
          actionName, displayName, usage, comment);
    }
  }
}
