package io.ray.streaming.state.store.memory;

import io.ray.streaming.common.metric.MetricGroup;
import io.ray.streaming.state.backend.memory.MemoryStateBackend;
import io.ray.streaming.state.typeinfo.serializer.TypeSerializer;
import java.util.HashMap;
import java.util.Map;

public class MemoryNonKeyedValueStore<V> extends MemoryValueStore<V> {

  private Map<Object, V> storeBackend = new HashMap<>();

  public MemoryNonKeyedValueStore(MemoryStateBackend backend,
                                   String jobName,
                                   String stateName,
                                   TypeSerializer typeSerializer,
                                   MetricGroup metricGroup,
                                   int stateKey) {
    super(backend, jobName, stateName, typeSerializer, metricGroup);
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
