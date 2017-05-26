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
package org.smartdata.server.actions.mover;

import java.util.UUID;

/**
 * Manage the status of a moving action.
 */
public abstract class Status {

  public abstract UUID getId();

  public abstract Boolean isFinished();

  public abstract void setFinished();

  public abstract long getStartTime();

  public abstract void setStartTime(long startTime);

  public abstract Boolean isSuccessful();

  public abstract void setSuccessful();

  public abstract void setTotalDuration(long totalDuration);

  public abstract long getRunningTime();

  public abstract void reset();

  public abstract long getTotalSize();

  public abstract float getPercentage();
}
