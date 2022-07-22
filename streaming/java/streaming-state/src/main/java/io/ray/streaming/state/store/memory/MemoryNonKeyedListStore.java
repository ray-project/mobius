package io.ray.streaming.state.store.memory;

import io.ray.streaming.common.metric.MetricGroup;
import io.ray.streaming.state.api.state.ListState;
import io.ray.streaming.state.store.backend.memory.MemoryStateBackend;
import java.util.ArrayList;
import java.util.List;

/** Description <br> */
public class MemoryNonKeyedListStore<V> extends AbstractMemoryStore implements ListState<V> {

  private final List<V> list;

  public MemoryNonKeyedListStore(
      MemoryStateBackend backend, String jobName, String stateName, MetricGroup metricGroup) {
    super(jobName, stateName, metricGroup);
    list = new ArrayList<>();
  }

  @Override
  public void add(V ele) {
    //    writeMeter.update();
    list.add(ele);
  }

  @Override
  public void addAll(List<V> list) {
    for (V v : list) {
      add(v);
    }
  }

  @Override
  public V get(int index) {
    //    readMeter.update();
    return list.get(index);
  }

  @Override
  public V remove(int index) {
    //    deleteMeter.update();
    return list.remove(index);
  }

  @Override
  public boolean isEmpty() {
    return list.isEmpty();
  }

  @Override
  public int size() {
    return list.size();
  }

  @Override
  public void clear() {
    list.clear();
  }

  @Override
  public void set(int index, V ele) throws Exception {
    list.set(index, ele);
  }
}
