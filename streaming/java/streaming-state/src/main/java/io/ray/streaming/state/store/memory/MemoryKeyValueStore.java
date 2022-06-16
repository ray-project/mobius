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

package io.ray.streaming.state.store.memory;

import io.ray.streaming.common.metric.MetricGroup;
import io.ray.streaming.state.backend.memory.MemoryStateBackend;
import io.ray.streaming.state.keystate.state.KeyValueState;
import io.ray.streaming.state.typeinfo.serializer.TypeSerializer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Implementation of {@link KeyValueState} type state in Memory backend.
 *
 * @param <K> Key data type
 * @param <V> Value data type
 */
public class MemoryKeyValueStore<K, V> extends AbstractMemoryStore implements KeyValueState<K, V> {

  protected MemoryStateBackend backend;
  protected Map<K, V> storeBackend = new HashMap<>();

  public MemoryKeyValueStore(MemoryStateBackend backend,
      String jobName,
      String stateName,
      TypeSerializer typeSerializer,
      MetricGroup metricGroup) {

    super(jobName, stateName, metricGroup);
    this.backend = backend;
  }

  @Override
  public V get(K key) throws Exception {
    readMeter.update(1);

    return storeBackend.get(key);
  }

  @Override
  public void put(K key, V value) throws Exception {
    writeMeter.update(1);

    storeBackend.put(key, value);
  }

  @Override
  public void putAll(Map<K, V> map) throws Exception {
    if (map == null) {
      return;
    }
    writeMeter.update(map.size());

    storeBackend.putAll(map);
  }

  @Override
  public void remove(K key) throws Exception {
    deleteMeter.update(1);
    storeBackend.remove(key);
  }

  @Override
  public boolean contains(K key) throws Exception {
    return storeBackend.containsKey(key);
  }

  @Override
  public Iterable<Entry<K, V>> entries() throws Exception {
    return storeBackend.entrySet();
  }

  @Override
  public Iterable<K> keys() throws Exception {
    return storeBackend.keySet();
  }

  @Override
  public Iterable<V> values() throws Exception {
    return storeBackend.values();
  }

  @Override
  public Iterator<Entry<K, V>> iterator() throws Exception {
    return entries().iterator();
  }

  @Override
  public void clear() {
    storeBackend.clear();
  }
}
