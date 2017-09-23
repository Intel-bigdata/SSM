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
package org.smartdata.hdfs.action.move;

/**
 * ActionStatus of Mover tool.
 */
public class MoverStatus {
  private int totalBlocks; // each replica is counted as a block
  private int movedBlocks;

  public MoverStatus() {
    totalBlocks = 0;
    movedBlocks = 0;
  }

  public float getPercentage() {
    if (totalBlocks == 0) {
      return 0;
    }
    return movedBlocks * 1.0F / totalBlocks;
  }

  public int getTotalBlocks() {
    return totalBlocks;
  }

  public void setTotalBlocks(int blocks) {
    totalBlocks = blocks;
  }

  synchronized public int increaseMovedBlocks(int blocks) {
    movedBlocks += blocks;
    return movedBlocks;
  }

  public int getMovedBlocks() {
    return movedBlocks;
  }
}
