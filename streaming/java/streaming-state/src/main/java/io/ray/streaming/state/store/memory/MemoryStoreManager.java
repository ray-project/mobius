package io.ray.streaming.state.store.memory;

import io.ray.streaming.common.metric.MetricGroup;
import io.ray.streaming.state.RollbackSnapshotResult;
import io.ray.streaming.state.SnapshotResult;
import io.ray.streaming.state.api.desc.ListStateDescriptor;
import io.ray.streaming.state.api.desc.MapStateDescriptor;
import io.ray.streaming.state.api.desc.ValueStateDescriptor;
import io.ray.streaming.state.api.state.HiddenKeyState;
import io.ray.streaming.state.api.state.ListState;
import io.ray.streaming.state.api.state.MapState;
import io.ray.streaming.state.api.state.ValueState;
import io.ray.streaming.state.backend.memory.MemoryStateBackend;
import io.ray.streaming.state.store.Store;
import io.ray.streaming.state.store.StoreManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Memory state backend.
 */
public class MemoryStoreManager implements StoreManager{

  private static final Logger LOG = LoggerFactory.getLogger(MemoryStoreManager.class);
  private static final int UNIQUE_STATE_KEY = -1;
  private final String jobName;
  private final String stateName;

  private MemoryStateBackend memoryStateBackend;

  private List<Store> storeList = new ArrayList<>();

  private MetricGroup metricGroup;

  public MemoryStoreManager(String jobName,
                            String stateName,
                            Map<String, String> stateConfig,
                            MetricGroup metricGroup) {

    this.memoryStateBackend = new MemoryStateBackend();
    this.jobName = jobName;
    this.stateName = stateName;
    this.metricGroup = metricGroup;
    LOG.info("Create memory store manager success.");
  }

  @Override
  public <K, V> MapState<K, V> buildMapStore(MapStateDescriptor<K, V> descriptor) {
    MemoryMapStore<K, V> keyValueStore = new MemoryMapStore<>(memoryStateBackend,
        jobName,
        stateName,
        null,
        metricGroup);

    storeList.add(keyValueStore);
    LOG.info("Build key-value state, descriptor: {}", descriptor);
    return keyValueStore;
  }

  @Override
  public <K, V> MapState<K, V> buildNonKeyedMapStore(
          MapStateDescriptor<K, V> descriptor) {
    MemoryNonKeyedMapStore<K, V> nonKeyedMapStore = new MemoryNonKeyedMapStore<>(memoryStateBackend,
            jobName,
            stateName,
            null,
            metricGroup,
        UNIQUE_STATE_KEY);

    storeList.add(nonKeyedMapStore);
    LOG.info("Build nonKeyed key-value state, descriptor: {}", descriptor);
    return nonKeyedMapStore;
  }

  @Override
  public <V> ValueState<V> buildValueStore(ValueStateDescriptor<V> descriptor) {
    MemoryValueStore<V> valueStore = new MemoryValueStore<>(memoryStateBackend,
        jobName,
        stateName,
        null,
        metricGroup);

    storeList.add(valueStore);
    LOG.info("Build value state, descriptor: {}", descriptor);
    return valueStore;
  }

  @Override
  public <V> ValueState<V> buildNonKeyedValueStore(ValueStateDescriptor<V> descriptor) {
    MemoryNonKeyedValueStore<V> valueStore = new MemoryNonKeyedValueStore<>(memoryStateBackend,
            jobName,
            stateName,
            null,
            metricGroup,
        UNIQUE_STATE_KEY);

    storeList.add(valueStore);
    LOG.info("Build value state, descriptor: {}", descriptor);
    return valueStore;
  }

  @Override
  public <V> ListState<V> buildNonKeyedListStore(ListStateDescriptor<V> descriptor) {
    MemoryNonKeyedListStore<V> listStore = new MemoryNonKeyedListStore<>(memoryStateBackend,
            jobName,
            stateName,
            metricGroup);
    storeList.add(listStore);
    LOG.info("Build value state, descriptor: {}", descriptor);
    return listStore;
  }

  @Override
  public void setCurrentKey(Object key) {
    for (Store store : storeList) {
      if (store instanceof HiddenKeyState) {
        HiddenKeyState hiddenKeyState = (HiddenKeyState) store;
        hiddenKeyState.setCurrentKey(key);
      }
    }
  }

  @Override
  public CompletableFuture<SnapshotResult> snapshot(long snapshotId) {
    CompletableFuture<Boolean> snapshotFuture = memoryStateBackend.snapshot(snapshotId);
    try {
      //sync flush
      snapshotFuture.get();
    } catch (Exception e) {
      throw new RuntimeException("Doing snapshot failed.", e);
    }
    return CompletableFuture.supplyAsync(() -> new SnapshotResult(snapshotId, true));
  }

  @Override
  public CompletableFuture<RollbackSnapshotResult> rollbackSnapshot(long snapshotId) {
    return CompletableFuture.supplyAsync(() -> new RollbackSnapshotResult(snapshotId, true));
  }

  @Override
  public boolean deleteSnapshot(long expiredCheckpointId) {
    //TODO
    LOG.info("Clean expired checkpoint id: {}.", expiredCheckpointId);
    return true;
  }

  @Override
  public void close() {

  }
}