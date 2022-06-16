package io.ray.streaming.state.store.memory;

import io.ray.streaming.common.metric.MetricGroup;
import io.ray.streaming.state.backend.memory.MemoryStateBackend;
import io.ray.streaming.state.keystate.state.ValueState;
import io.ray.streaming.state.typeinfo.serializer.TypeSerializer;
import java.util.HashMap;
import java.util.Map;

public class MemoryValueStore<V> extends AbstractMemoryStore implements ValueState<V> {

  private MemoryStateBackend backend;

  private Map<Object, V> storeBackend = new HashMap<>();

  protected Object currentKey;

  public MemoryValueStore(MemoryStateBackend backend,
                          String jobName,
                          String stateName,
                          TypeSerializer typeSerializer,
                          MetricGroup metricGroup) {
    super(jobName, stateName, metricGroup);

    this.backend = backend;
  }

  @Override
  public Object getCurrentKey() {
    return currentKey;
  }

  @Override
  public void setCurrentKey(Object currentKey) {
    this.currentKey = currentKey;
  }

  @Override
  public V value() throws Exception {
    readMeter.update(1);

    return storeBackend.get(currentKey);
  }

  @Override
  public void update(V value) throws Exception {
    writeMeter.update(1);

    storeBackend.put(currentKey, value);
  }
}
