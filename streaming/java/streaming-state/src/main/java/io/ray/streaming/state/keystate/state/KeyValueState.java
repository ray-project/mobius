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

package io.ray.streaming.state.keystate.state;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

/** KeyValueState interface. */
public interface KeyValueState<K, V> extends KeyedState {

  /**
   * Returns the current value associated with the given key.
   *
   * @param key The key of the mapping
   * @return The value of the mapping with the given key
   */
  V get(K key) throws Exception;

  /**
   * Associates a new value with the given key.
   *
   * @param key The key of the mapping
   * @param value The new value of the mapping
   */
  void put(K key, V value) throws Exception;

  /**
   * Copies all of the mappings from the given map into the state.
   *
   * @param map The mappings to be stored in this state
   */
  void putAll(Map<K, V> map) throws Exception;

  /**
   * Deletes the mapping of the given key.
   *
   * @param key The key of the mapping
   */
  void remove(K key) throws Exception;

  /**
   * Returns whether there exists the given mapping.
   *
   * @param key The key of the mapping
   * @return True if there exists a mapping whose key equals to the given key
   */
  boolean contains(K key) throws Exception;

  /**
   * Returns all the mappings in the state
   *
   * @return An iterable view of all the key-value pairs in the state.
   */
  Iterable<Entry<K, V>> entries() throws Exception;

  /**
   * Returns all the keys in the state
   *
   * @return An iterable view of all the keys in the state.
   */
  Iterable<K> keys() throws Exception;

  /**
   * Returns all the values in the state.
   *
   * @return An iterable view of all the values in the state.
   */
  Iterable<V> values() throws Exception;

  /**
   * Iterates over all the mappings in the state.
   *
   * @return An iterator over all the mappings in the state
   */
  Iterator<Entry<K, V>> iterator() throws Exception;

  /**
   * Removes current key-value state of all data.
   */
  void clear();
}
