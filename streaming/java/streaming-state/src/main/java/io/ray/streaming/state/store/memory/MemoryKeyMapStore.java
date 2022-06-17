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
import io.ray.streaming.state.keystate.state.KeyMapState;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;


public class MemoryKeyMapStore<K, UK, UV> extends AbstractMemoryStore implements
    KeyMapState<K, UK, UV> {

  private Map<K, Map<UK, UV>> storeBackend = new ConcurrentHashMap<>();

  public MemoryKeyMapStore(MemoryStateBackend backend,
      String jobName,
      String stateName,
      MetricGroup metricGroup) {

    super(jobName, stateName, metricGroup);
  }

  @Override
  public UV get(K key, UK subKey) throws Exception {
    readMeter.update(1);

    if (storeBackend.containsKey(key) && storeBackend.get(key) != null) {
      return storeBackend.get(key).get(subKey);
    }
    return null;
  }

  @Override
  public Map<UK, UV> multiGet(K key) throws Exception {
    readMeter.update(1);

    return storeBackend.get(key);
  }

  @Override
  public void put(K key, UK subKey, UV subValue) throws Exception {
    if (key == null || subKey == null) {
      return;
    }
    writeMeter.update(1);

    if (storeBackend.containsKey(key) && storeBackend.get(key) != null) {
      storeBackend.get(key).put(subKey, subValue);
    } else {
      Map<UK, UV> subMap = new HashMap<>();
      subMap.put(subKey, subValue);
      storeBackend.put(key, subMap);
    }
  }

  @Override
  public void multiPut(K key, Map<UK, UV> value) throws Exception {
    if (value == null) {
      return;
    }
    writeMeter.update(value.size());

    if (storeBackend.containsKey(key) && storeBackend.get(key) != null) {
      storeBackend.get(key).putAll(value);
    } else {
      Map<UK, UV> subMap = new HashMap<>(value.size());
      for (Map.Entry<UK, UV> map : value.entrySet()) {
        subMap.put(map.getKey(), map.getValue());
      }
      storeBackend.put(key, subMap);
    }
  }

  @Override
  public void remove(K key, UK subKey) throws Exception {
    deleteMeter.update(1);

    if (storeBackend.containsKey(key) && storeBackend.get(key) != null) {
      storeBackend.get(key).remove(subKey);

      if (storeBackend.get(key).size() == 0) {
        storeBackend.remove(key);
      }
    }
  }

  @Override
  public void removeAll(K key) throws Exception {
    storeBackend.remove(key);
  }

  @Override
  public boolean contains(K key, UK subKey) throws Exception {
    if (storeBackend.containsKey(key) && storeBackend.get(key) != null) {
      return storeBackend.get(key).containsKey(subKey);
    }
    return false;
  }

  @Override
  public boolean isEmpty(K key) throws Exception {
    if (storeBackend.containsKey(key) && storeBackend.get(key) != null) {
      return storeBackend.get(key).size() == 0;
    }
    return true;
  }

  @Override
  public Iterator<Entry<UK, UV>> iterator(K key) throws Exception {
    Iterable<Entry<UK, UV>> entries = entries(key);
    if (entries != null) {
      return entries.iterator();
    }
    return null;
  }

  @Override
  public Iterable<UK> keys(K key) throws Exception {
    if (storeBackend.containsKey(key) && storeBackend.get(key) != null) {
      return storeBackend.get(key).keySet();
    }
    return null;
  }

  @Override
  public Iterable<UV> values(K key) throws Exception {
    if (storeBackend.containsKey(key) && storeBackend.get(key) != null) {
      return storeBackend.get(key).values();
    }
    return null;
  }

  @Override
  public Iterable<Entry<UK, UV>> entries(K key) throws Exception {
    if (storeBackend.containsKey(key)) {
      return storeBackend.get(key).entrySet();
    }
    return null;
  }
}
