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
  private long totalBlocks;
  private long totalSize;
  private long movedBlocks;
  private boolean totalValueSet;

  public MoverStatus() {
    totalBlocks = 0;
    totalSize = 0;
    movedBlocks = 0;
    totalValueSet = false;
  }

  synchronized public float getPercentage() {
    if (!totalValueSet) {
      return 0;
    }
    return movedBlocks * 1.0F / totalBlocks;
  }

  synchronized public long getTotalSize() {
    return totalSize;
  }

  synchronized public long getTotalBlocks() {
    return totalBlocks;
  }

  synchronized public void setTotalBlocks(long blocks) {
    if (totalValueSet) {
      return;
    }
    totalBlocks = blocks;
  }

  synchronized public long increaseTotalBlocks(long blocks) {
    if (totalValueSet) {
      return totalBlocks;
    }
    totalBlocks += blocks;
    return totalBlocks;
  }

  synchronized public void setTotalSize(long size) {
    if (totalValueSet) {
      return;
    }
    totalSize = size;
  }

  synchronized public long increaseTotalSize(long size) {
    if (totalValueSet) {
      return totalSize;
    }
    totalSize += size;
    return totalSize;
  }

  synchronized public long increaseMovedBlocks(long blocks) {
    movedBlocks += blocks;
    return movedBlocks;
  }

  synchronized public void setMovedBlocks(long blocks) {
    movedBlocks = blocks;
  }

  synchronized public long getMovedBlocks() {
    return movedBlocks;
  }

  synchronized public void completeTotalValueSet() {
    totalValueSet = true;
  }
}
