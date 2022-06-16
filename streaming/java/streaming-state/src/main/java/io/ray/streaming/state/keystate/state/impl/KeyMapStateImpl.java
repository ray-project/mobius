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

package io.ray.streaming.state.keystate.state.impl;

import io.ray.streaming.state.keystate.desc.KeyMapStateDescriptor;
import io.ray.streaming.state.keystate.state.KeyMapState;
import java.util.HashMap;
import java.util.Map;

/** KeyValueState implementation. */
public class KeyMapStateImpl<K, UK, UV> implements KeyMapState<K, UK, UV> {

  private final StateHelper<Map<K, Map<UK, UV>>> helper;

  public KeyMapStateImpl(KeyMapStateDescriptor<K, UK, UV> descriptor, KeyStateBackend backend) {
    this.helper = new StateHelper<>(backend, descriptor);
  }

  @Override
  public Map<K, Map<UK, UV>> get() {
    Map<K, Map<UK, UV>> map = helper.get();
    if (map == null) {
      map = new HashMap<>();
    }
    return map;
  }

  @Override
  public Map<UK, UV> get(K key) {
    Map<K, Map<UK, UV>> map = get();
    return map.get(key);
  }

  @Override
  public void put(K key, Map<UK, UV> value) {
    Map<K, Map<UK, UV>> map = get();

    map.put(key, value);
    helper.put(map);
  }

  @Override
  public void update(Map<K, Map<UK, UV>> map) {
    if (map == null) {
      map = new HashMap<>();
    }
    helper.put(map);
  }

  @Override
  public void putAll(Map<K, Map<UK, UV>> newMap) {
    Map<K, Map<UK, UV>> map = get();

    map.putAll(newMap);
    helper.put(map);
  }

  @Override
  public void remove(K key) {
    Map<K, Map<UK, UV>> map = get();

    map.remove(key);
    helper.put(map);
  }

  /** set current key of the state */
  @Override
  public void setCurrentKey(Object currentKey) {
    helper.setCurrentKey(currentKey);
  }
}
