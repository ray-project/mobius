/*
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

package io.ray.streaming.state.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.hash.Hashing;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** This class defines key-group assignment algorithm。 */
public final class KeyGroupAssignment {

  /**
   * Computes the range of key-groups that are assigned to a given operator under the given
   * parallelism and maxShard.
   *
   * @param maxShard Maximal shard that the job was initially created with.
   * @param parallelism    The current parallelism under which the job runs. Must be <=
   *                       shard.
   * @param index          Id of a key-group. 0 <= keyGroupID < maxParallelism.
   */
  public static KeyGroup computeKeyGroupRangeForOperatorIndex(int maxShard, int parallelism,
      int index) {

    Preconditions.checkArgument(maxShard >= parallelism,
        "maxShard (%s) must not be smaller than parallelism(%s)", maxShard,
        parallelism);

    int start = index == 0 ? 0 : ((index * maxShard - 1) / parallelism) + 1;
    int end = ((index + 1) * maxShard - 1) / parallelism;
    return new KeyGroup(start, end, maxShard);
  }

  /**
   * Assigns the given key to a key-group index.
   *
   * @param key            the key to assign
   * @param maxShard the maximum supported parallelism, aka the number of key-groups.
   * @return the key-group to which the given key is assigned
   */
  public static int assignToKeyGroup(Object key, int maxShard) {
    return computeKeyGroupForKeyHash(key.hashCode(), maxShard);
  }

  /**
   * Assigns the given key to a key-group index.
   *
   * @param keyHash        the hash of the key to assign
   * @param maxShard the maximum supported parallelism, aka the number of key-groups.
   * @return the key-group to which the given key is assigned
   */
  public static int computeKeyGroupForKeyHash(int keyHash, int maxShard) {
    // hash code for Number is not friendly for key group.
    // For example, if all keys are int and in range [0,127), when key group interval is larger
    // than [0,127), all key will be located in the same key group or adjacent key groups which
    // is uneven.
    int rehash = Hashing.murmur3_32().hashInt(keyHash).asInt();
    return Math.abs(rehash % maxShard);
  }

  public static Map<Integer, List<Integer>> computeKeyGroupToTask(int maxShard,
      List<Integer> targetTasks) {
    Map<Integer, List<Integer>> keyGroupToTask = new ConcurrentHashMap<>();
    for (int index = 0; index < targetTasks.size(); index++) {
      KeyGroup taskKeyGroup = computeKeyGroupRangeForOperatorIndex(maxShard,
          targetTasks.size(), index);
      for (int groupId = taskKeyGroup.getStartKeyGroup();
          groupId <= taskKeyGroup.getEndKeyGroup(); groupId++) {
        keyGroupToTask.put(groupId, ImmutableList.of(targetTasks.get(index)));
      }
    }
    return keyGroupToTask;
  }

  /**
   * Converts a byte array to a unsigned short value (KeyGroupId)
   * @param bytes the byte array
   * @return
   */
  public static int bytesToKeyGroup(byte[] bytes) {
    return bytesToKeyGroup(bytes, 0);
  }

  public static int bytesToKeyGroup(byte[] bytes, int offset) {
    int n = 0;
    n ^= bytes[offset] & 0xFF;
    n <<= 8;
    n ^= bytes[offset+1] & 0xFF;
    return n;
  }
}
