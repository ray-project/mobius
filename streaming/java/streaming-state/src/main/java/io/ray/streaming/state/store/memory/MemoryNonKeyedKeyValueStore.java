package io.ray.streaming.state.store.memory;

import io.ray.streaming.common.metric.MetricGroup;
import io.ray.streaming.state.backend.memory.MemoryStateBackend;
import io.ray.streaming.state.keystate.state.KeyValueState;

/**
 * Implementation of {@link KeyValueState} type(nonkeyed) state in Memory backend.
 *
 * @param <K> Key data type
 * @param <V> Value data type
 */
public class MemoryNonKeyedKeyValueStore<K, V> extends MemoryKeyValueStore<K, V> {

  private final int stateKey;
  public MemoryNonKeyedKeyValueStore(MemoryStateBackend backend,
                                      String jobName,
                                      String stateName,
                                      MetricGroup metricGroup,
                                      int stateKey) {

    super(backend, jobName, stateName, metricGroup);
    this.stateKey = stateKey;
  }
}
