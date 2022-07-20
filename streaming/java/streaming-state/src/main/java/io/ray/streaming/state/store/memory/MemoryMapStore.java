package io.ray.streaming.state.store.memory;

import io.ray.streaming.common.metric.MetricGroup;
import io.ray.streaming.state.api.state.MapState;
import io.ray.streaming.state.store.backend.memory.MemoryStateBackend;
import io.ray.streaming.state.typeinfo.serializer.TypeSerializer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Implementation of {@link MapState} type state in Memory backend.
 *
 * @param <K> Key data type
 * @param <V> Value data type
 */
public class MemoryMapStore<K, V> extends AbstractMemoryStore implements MapState<K, V> {

  protected MemoryStateBackend backend;
  protected Map<K, V> storeBackend = new HashMap<>();

  public MemoryMapStore(MemoryStateBackend backend,
                             String jobName,
                             String stateName,
                             TypeSerializer typeSerializer,
                             MetricGroup metricGroup) {

    super(jobName, stateName, metricGroup);
    this.backend = backend;
  }

  @Override
  public V get(K key) throws Exception {
//    readMeter.update(1);

    return storeBackend.get(key);
  }

  @Override
  public void put(K key, V value) throws Exception {
//    writeMeter.update(1);

    storeBackend.put(key, value);
  }

  @Override
  public void putAll(Map<K, V> map) throws Exception {
    if (map == null) {
      return;
    }
//    writeMeter.update(map.size());

    storeBackend.putAll(map);
  }

  @Override
  public void remove(K key) throws Exception {
//    deleteMeter.update(1);
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
