package io.ray.streaming.state.manager;

import com.google.common.base.Preconditions;
import io.ray.streaming.common.metric.MetricGroup;
import io.ray.streaming.state.RollbackSnapshotResult;
import io.ray.streaming.state.SnapshotResult;
import io.ray.streaming.state.api.desc.ListStateDescriptor;
import io.ray.streaming.state.api.desc.MapStateDescriptor;
import io.ray.streaming.state.api.desc.ValueStateDescriptor;
import io.ray.streaming.state.api.state.ListState;
import io.ray.streaming.state.api.state.MapState;
import io.ray.streaming.state.api.state.State;
import io.ray.streaming.state.api.state.ValueState;
import io.ray.streaming.state.buffer.MemoryManager;
import io.ray.streaming.state.config.StateConfig;
import io.ray.streaming.state.store.StateStoreFactory;
import io.ray.streaming.state.store.StoreManager;
import io.ray.streaming.state.store.StoreStatus;
import io.ray.streaming.state.typeinfo.serializer.TypeSerializerConfig;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.aeonbits.owner.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * StateManager abstract base class.
 */
public abstract class AbstractStateManager {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractStateManager.class);
  private static final Object lock = new Object();
  protected final String jobName;
  protected final String operatorName;
  protected final int taskParallelism;
  protected final int taskIndex;
  protected final int maxShard;
  protected final Map<String, String> config;
  protected final StateConfig stateConfig;
  private final MemoryManager memoryManager;
  private final TypeSerializerConfig serializerConfig;
  private final MetricGroup metricGroup;
//  protected KeyGroup keyGroup;
  protected StoreManager storeManager;
  //for value state
  private Object currentKey;

  public AbstractStateManager(final String jobName,
      final String operatorName,
      final int taskParallelism,
      final int taskIndex,
      final Map<String, String> config,
      final TypeSerializerConfig serializerConfig,
      final MetricGroup metricGroup) {

    LOG.info("Begin creating RayState.");
    this.jobName = jobName;
    this.operatorName = operatorName;
    this.taskParallelism = taskParallelism;
    this.taskIndex = taskIndex;
    this.config = config;
    if (config.containsKey(StateConfig.BACKEND_TYPE)) {
      String backendValue = config.get(StateConfig.BACKEND_TYPE);
      config.put(StateConfig.BACKEND_TYPE, backendValue.toUpperCase());
      LOG.info("State backend type: {}", config.get(StateConfig.BACKEND_TYPE));
    }

    this.stateConfig = ConfigFactory.create(StateConfig.class, config);
    maxShard = stateConfig.maxShard();
    Preconditions.checkArgument(maxShard <= StateConfig.STATE_MAX_SHARD_DEFAULT_VALUE,
            "%s[%s] must <= %s", StateConfig.STATE_MAX_SHARD, maxShard,
            StateConfig.STATE_MAX_SHARD_DEFAULT_VALUE);
//    this.keyGroup = KeyGroupAssignment.computeKeyGroupRangeForOperatorIndex(
//        maxShard,
//        taskParallelism,
//        taskIndex);

    this.metricGroup = metricGroup;

    this.memoryManager = new MemoryManager(config);

    this.serializerConfig = serializerConfig;
  }

  public Object getCurrentKey() {
    return currentKey;
  }

  public void setCurrentKey(Object currentKey) {
    this.currentKey = currentKey;
    if (storeManager != null) {
      storeManager.setCurrentKey(currentKey);
    }
  }

  /**
   * Take a async snapshot of the current state.
   *
   * @param snapshotId snapshot id.
   */
  public CompletableFuture<SnapshotResult> snapshot(long snapshotId) {
    LOG.info("Begin doing snapshot, snapshot id:{}.", snapshotId);
    if (storeManager != null) {
      return storeManager.snapshot(snapshotId);
    } else {
      return CompletableFuture.supplyAsync(() -> {
        LOG.warn("StateManager has not been initialized.");
        return new SnapshotResult(snapshotId, true);
      });
    }
  }

  /**
   * Restores state that was previously snapshot from the provided checkpointId.
   *
   * @param snapshotId snapshotId id.
   */
  public CompletableFuture<RollbackSnapshotResult> rollbackSnapshot(long snapshotId) {
    LOG.info("Begin doing rollback snapshot, snapshot id: {}.", snapshotId);
    if (storeManager != null) {
      return storeManager.rollbackSnapshot(snapshotId);
    } else {
      return CompletableFuture.supplyAsync(() -> {
        LOG.warn("StateManager has not been initialized.");
        return new RollbackSnapshotResult(snapshotId, true);
      });
    }
  }

  /**
   * Close the current resource handle and synchronize the memory state to backend.
   */
  public void close() {
    LOG.info("State close.");
    if (storeManager != null) {
      storeManager.close();
    }
  }

  protected void buildStoreManager() {
    if (storeManager == null) {
      synchronized (lock) {
        if (storeManager == null) {
          storeManager = StateStoreFactory.build(stateConfig.stateBackendType(),
              jobName, operatorName, config, metricGroup);
        }
      }
    }
  }

  public boolean deleteSnapshot(long snapshotId) {
    if (storeManager != null) {
      return storeManager.deleteSnapshot(snapshotId);
    } else {
      LOG.warn("StateManager has not been initialized.");
      return true;
    }
  }

  public StoreStatus getStoreStatus() {
    if (storeManager != null) {
      return storeManager.getStoreStatus();
    }
    return null;
  }

  public abstract <K, V> MapState<K, V> getMapState(
      MapStateDescriptor<K, V> stateDescriptor);

  public abstract <K, V> MapState<K, V> getNonKeyedMapState(
      MapStateDescriptor<K, V> stateDescriptor);

  public abstract <V> ValueState<V> getValueState(ValueStateDescriptor<V> stateDescriptor);

  public abstract <V> ValueState<V> getNonKeyedValueState(ValueStateDescriptor<V> stateDescriptor);

  public abstract <V> ListState<V> getNonKeyedListState(ListStateDescriptor<V> stateDescriptor);

  public abstract Map<String, State> getStateMap();
}