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

package io.ray.streaming.state.api.state;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

/** MapState interface. */
public interface MapState<K, V> extends KeyedState {

  /**
   * Get the value corresponding to key.
   *
   * @param key The key of the key-value.
   * @return The value of the key-value with the given key.
   */
  V get(K key) throws Exception;

  /**
   * Associates a new value with the give key.
   *
   * @param key The key of the key-value.
   * @param value The new value of the key-value.
   */
  void put(K key, V value) throws Exception;

  /**
   * Copies all of the mappings from given map into the state.
   *
   * @param map The mappings to be stored in this state.
   */
  void putAll(Map<K, V> map) throws Exception;

  /**
   * Deletes the mapping of the give key.
   *
   * @param key The key of the key-value.
   */
  void remove(K key) throws Exception;

  /**
   * Return whether there exists given mapping.
   *
   * @param key The key of the key-value.
   * @return whether the map state contains this state or not
   */
  boolean contains(K key) throws Exception;

  /**
   * Return all the mappings in the state.
   *
   * @return An iterator over all the key-value in the state.
   */
  Iterable<Entry<K, V>> entries() throws Exception;

  /**
   * Return all the keys in the state.
   *
   * @return An iterable view of all the keys in the state.
   */
  Iterable<K> keys() throws Exception;

  /**
   * Return all the values in the state.
   *
   * @return An iterable view of all the values in the state.
   */
  Iterable<V> values() throws Exception;

  /**
   * Iterates over all the mappings in the state.
   *
   * @return An iterable over all the key-value in the state.
   */
  Iterator<Entry<K, V>> iterator() throws Exception;

  /** Removes current key-value state of all data. */
  void clear();
}
