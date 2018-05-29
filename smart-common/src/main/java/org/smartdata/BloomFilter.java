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
package org.smartdata;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public class BloomFilter {
  private static final int BIT_SIZE = 1 << 28;
  private final int[] seeds = new int[] { 3, 5, 7, 11, 13, 31, 37, 61 };
  private BitSet bitSet = new BitSet(BIT_SIZE);
  private BloomHash[] bloomHashes = new BloomHash[seeds.length];
  private List<String> whiteList = new ArrayList<>();

  public BloomFilter() {
    for (int i = 0; i < seeds.length; i++) {
      bloomHashes[i] = new BloomHash(BIT_SIZE, seeds[i]);
    }
  }

  /**
   * Add element to bloom filter.
   *
   * @param element the element
   */
  public void addElement(String element) {
    if (element == null) {
      return;
    }

    whiteList.remove(element);

    // Add element to bit set
    for (BloomHash bloomHash : bloomHashes) {
      bitSet.set(bloomHash.hash(element));
    }
  }

  /**
   * Delete element to bloom filter.
   *
   * @param element the excluded element
   */
  public void deleteElement(String element) {
    whiteList.add(element);
  }

  /**
   * Check if bloom filter contains the value.
   *
   * @param value the value to be checked
   */
  public boolean contains(String value) {
    if (value == null) {
      return false;
    }

    if (whiteList.contains(value)) {
      return false;
    }

    for (BloomHash bloomHash : bloomHashes) {
      if (!bitSet.get(bloomHash.hash(value))) {
        return false;
      }
    }

    return true;
  }

  private class BloomHash {
    private int size;
    private int seed;

    private BloomHash(int cap, int seed) {
      this.size = cap;
      this.seed = seed;
    }

    /**
     * Calculate the index of value in bit set through hash.
     */
    private int hash(String value) {
      int result = 0;
      int len = value.length();
      for (int i = 0; i < len; i++) {
        result = seed * result + value.charAt(i);
      }

      return (size - 1) & result;
    }
  }
}
