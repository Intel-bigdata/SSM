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
package org.apache.hadoop.ssm;

public enum ActionType {
  CACHE,      // Cache a file or directory
  ARCHIVE,    // Set storage policy to ARCHIVE and enforce policy
  SSD;        // Set storage policy to ONE_SSD and enforce policy

  public static ActionType getActionType(String str) {
    if (str.equals("cache"))
      return CACHE;
    else if (str.equals("archive"))
      return ARCHIVE;
    else if (str.equals("ssd"))
      return SSD;
    return null;
  }
}