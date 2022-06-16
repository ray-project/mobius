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

public interface KeyMapState<K, UK, UV> extends KeyedState {

  /**
   * Return the current value associated with the given keu and subKey.
   *
   * @param key    The key of the key-mapping
   * @param subKey The key of the mapping
   * @return The value of the mapping.
   */
  UV get(K key, UK subKey) throws Exception;

  /**
   * Return the current mapping value  associated with the given key.
   *
   * @param key The key of the key-mapping
   * @return The value of the key-value with the given key.
   */
  Map<UK, UV> multiGet(K key) throws Exception;

  /**
   * Associates a new value with the key and subKey.
   *
   * @param key      The key of the key-mapping
   * @param subKey   The key of the mapping
   * @param subValue The value of the mapping
   */
  void put(K key, UK subKey, UV subValue) throws Exception;

  /**
   * Copies all of the mappings from the given map into the state.
   *
   * @param key   The key of the key-mapping
   * @param value The mappings to be stored in this state
   */
  void multiPut(K key, Map<UK, UV> value) throws Exception;

  /**
   * Deletes the mapping of the given key and subKey.
   *
   * @param key    The key of the key-mapping
   * @param subKey The key of the key-mapping
   */
  void remove(K key, UK subKey) throws Exception;

  /**
   * Deletes the mapping of the given key.
   *
   * @param key The key of the key-mapping
   */
  void removeAll(K key) throws Exception;

  /**
   * Returns whether there exists the given mapping.
   *
   * @param key    The key of the key-mapping
   * @param subKey The key of the mapping
   * @return True if there exists a mapping those key and subKey equals to the given key and subKey.
   */
  boolean contains(K key, UK subKey) throws Exception;

  /**
   * Returns true if this state contains no data mappings, otherwise false.
   *
   * @param key The key of the key-mapping
   * @return True if this state contains no data mappings, otherwise false.
   */
  boolean isEmpty(K key) throws Exception;

  /**
   * Iterates over all the key-mappings in the state.
   *
   * @param key The key of the key-mapping
   * @return An iterator over all the mappings in the state.
   */
  Iterator<Entry<UK, UV>> iterator(K key) throws Exception;

  /**
   * Return all the subKeys in the state.
   *
   * @param key The key of the key-mapping
   * @return An iterable view of all the subKeys in the state.
   */
  Iterable<UK> keys(K key) throws Exception;

  /**
   * Returns all the values of the state.
   *
   * @param key The key of the key-mapping
   * @return An iterable view of all the values in the state.
   */
  Iterable<UV> values(K key) throws Exception;

  /**
   * Return all the mappings in the state.
   *
   * @param key The key of the key-mapping
   * @return An iterable view of all the key-value pairs in the state.
   */
  Iterable<Entry<UK, UV>> entries(K key) throws Exception;
}