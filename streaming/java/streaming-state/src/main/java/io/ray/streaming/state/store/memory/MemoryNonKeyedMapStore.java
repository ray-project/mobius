package io.ray.streaming.state.store.memory;

import io.ray.streaming.common.metric.MetricGroup;
import io.ray.streaming.state.api.state.MapState;
import io.ray.streaming.state.store.backend.memory.MemoryStateBackend;
import io.ray.streaming.state.typeinfo.serializer.TypeSerializer;

/**
 * Implementation of {@link MapState} type(nonkeyed) state in Memory backend.
 *
 * @param <K> Key data type
 * @param <V> Value data type
 */
public class MemoryNonKeyedMapStore<K, V> extends MemoryMapStore<K, V> {

  private final int stateKey;

  public MemoryNonKeyedMapStore(
      MemoryStateBackend backend,
      String jobName,
      String stateName,
      TypeSerializer typeSerializer,
      MetricGroup metricGroup,
      int stateKey) {

    super(backend, jobName, stateName, typeSerializer, metricGroup);
    this.stateKey = stateKey;
  }
}
