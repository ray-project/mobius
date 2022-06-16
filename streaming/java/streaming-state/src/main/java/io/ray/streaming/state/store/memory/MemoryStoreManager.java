package io.ray.streaming.state.store.memory;

import io.ray.streaming.common.metric.MetricGroup;
import io.ray.streaming.state.backend.memory.MemoryStateBackend;
import io.ray.streaming.state.keystate.desc.KeyMapStateDescriptor;
import io.ray.streaming.state.keystate.desc.KeyValueStateDescriptor;
import io.ray.streaming.state.keystate.desc.ValueStateDescriptor;
import io.ray.streaming.state.keystate.state.HiddenKeyState;
import io.ray.streaming.state.keystate.state.KeyMapState;
import io.ray.streaming.state.keystate.state.KeyValueState;
import io.ray.streaming.state.keystate.state.NonKeyedAble;
import io.ray.streaming.state.keystate.state.ValueState;
import io.ray.streaming.state.store.Store;
import io.ray.streaming.state.store.StoreManager;
import io.ray.streaming.state.RollbackSnapshotResult;
import io.ray.streaming.state.SnapshotResult;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Memory state backend.
 */
public class MemoryStoreManager implements StoreManager, NonKeyedAble {

  private static final Logger LOG = LoggerFactory.getLogger(MemoryStoreManager.class);

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
  public <K, V> KeyValueState<K, V> buildKeyValueStore(KeyValueStateDescriptor<K, V> descriptor) {
    MemoryKeyValueStore<K, V> keyValueStore = new MemoryKeyValueStore<>(memoryStateBackend,
        jobName,
        stateName,
        null,
        metricGroup);

    storeList.add(keyValueStore);
    LOG.info("Build key-value state, descriptor: {}", descriptor);
    return keyValueStore;
  }

  @Override
  public <K, V> KeyValueState<K, V> buildNonKeyedKeyValueStore(
          KeyValueStateDescriptor<K, V> descriptor) {
    MemoryNonKeyedKeyValueStore<K, V> nonKeyedKeyValueStore = new MemoryNonKeyedKeyValueStore<>(memoryStateBackend,
            jobName,
            stateName,
            null,
            metricGroup,
            getUniqueStateKey());

    storeList.add(nonKeyedKeyValueStore);
    LOG.info("Build nonKeyed key-value state, descriptor: {}", descriptor);
    return nonKeyedKeyValueStore;
  }

  @Override
  public <K, UK, UV> KeyMapState<K, UK, UV> buildKeyMapStore(
      KeyMapStateDescriptor<K, UK, UV> descriptor) {
    MemoryKeyMapStore<K, UK, UV> keyMapState = new MemoryKeyMapStore<>(memoryStateBackend,
        jobName,
        stateName,
        null,
        metricGroup);

    storeList.add(keyMapState);
    LOG.info("Build key-map state, descriptor: {}.", descriptor);
    return keyMapState;
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
            getUniqueStateKey());

    storeList.add(valueStore);
    LOG.info("Build value state, descriptor: {}", descriptor);
    return valueStore;
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
    return CompletableFuture.supplyAsync(() -> {
      return new RollbackSnapshotResult(snapshotId, true);
    });
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