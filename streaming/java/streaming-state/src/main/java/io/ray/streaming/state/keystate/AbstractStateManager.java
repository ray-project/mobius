package io.ray.streaming.state.keystate;

import com.google.common.base.Preconditions;
import io.ray.streaming.common.metric.MetricGroup;
import io.ray.streaming.state.RollbackSnapshotResult;
import io.ray.streaming.state.SnapshotResult;
import io.ray.streaming.state.config.StateConfig;
import io.ray.streaming.state.keystate.desc.KeyMapStateDescriptor;
import io.ray.streaming.state.keystate.desc.KeyValueStateDescriptor;
import io.ray.streaming.state.keystate.desc.ValueStateDescriptor;
import io.ray.streaming.state.keystate.state.KeyMapState;
import io.ray.streaming.state.keystate.state.KeyValueState;
import io.ray.streaming.state.keystate.state.State;
import io.ray.streaming.state.keystate.state.ValueState;
import io.ray.streaming.state.memory.MemoryManager;
import io.ray.streaming.state.store.StateStoreFactory;
import io.ray.streaming.state.store.StoreManager;
import io.ray.streaming.state.store.StoreStatus;
import io.ray.streaming.state.typeinfo.serializer.TypeSerializerConfig;
import io.ray.streaming.state.util.KeyGroup;
import io.ray.streaming.state.util.KeyGroupAssignment;
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
  protected KeyGroup keyGroup;
  protected StoreManager stateStoreManager;
  //TODO
  private Accessor accessor;
  private boolean useAccessor;
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
    this.keyGroup = KeyGroupAssignment.computeKeyGroupRangeForOperatorIndex(
        maxShard,
        taskParallelism,
        taskIndex);

    this.metricGroup = metricGroup;

    this.memoryManager = new MemoryManager(config);

    this.serializerConfig = serializerConfig;
  }

  public Object getCurrentKey() {
    return currentKey;
  }

  public void setCurrentKey(Object currentKey) {
    this.currentKey = currentKey;
    if (stateStoreManager != null) {
      stateStoreManager.setCurrentKey(currentKey);
    }
  }

  /**
   * Take a async snapshot of the current state.
   *
   * @param snapshotId snapshot id.
   */
  public CompletableFuture<SnapshotResult> snapshot(long snapshotId) {
    LOG.info("Begin doing snapshot, snapshot id:{}.", snapshotId);
    if (useAccessor) {
      return accessor.snapshot(snapshotId);
    } else {
      if (stateStoreManager != null) {
        return stateStoreManager.snapshot(snapshotId);
      } else {
        return CompletableFuture.supplyAsync(() -> {
          LOG.warn("StateManager has not been initialized.");
          return new SnapshotResult(snapshotId, true);
        });
      }
    }
  }

  /**
   * Restores state that was previously snapshot from the provided checkpointId.
   *
   * @param snapshotId snapshotId id.
   */
  public CompletableFuture<RollbackSnapshotResult> rollbackSnapshot(long snapshotId) {
    LOG.info("Begin doing rollback snapshot, snapshot id: {}.", snapshotId);
    if (useAccessor) {
      return accessor.rollback(snapshotId);
    } else {
      if (stateStoreManager != null) {
        return stateStoreManager.rollbackSnapshot(snapshotId);
      } else {
        return CompletableFuture.supplyAsync(() -> {
          LOG.warn("StateManager has not been initialized.");
          return new RollbackSnapshotResult(snapshotId, true);
        });
      }
    }
  }

  /**
   * Close the current resource handle and synchronize the memory state to backend.
   */
  public void close() {
    LOG.info("State close.");
    if (useAccessor) {
      accessor.close();
    } else {
      if (stateStoreManager != null) {
        stateStoreManager.close();
      }
    }
  }

  protected void buildStoreManager() {
    if (stateStoreManager == null) {
      synchronized (lock) {
        if (stateStoreManager == null) {
          stateStoreManager = StateStoreFactory.build(stateConfig.stateBackendType(),
              jobName, operatorName, taskIndex, keyGroup, config, memoryManager, metricGroup,
              serializerConfig);
        }
      }
    }
  }

  public boolean deleteSnapshot(long snapshotId) {
    if (stateStoreManager != null) {
      return stateStoreManager.deleteSnapshot(snapshotId);
    } else {
      LOG.warn("StateManager has not been initialized.");
      return true;
    }
  }

  public StoreStatus getStoreStatus() {
    if (stateStoreManager != null) {
      return stateStoreManager.getStoreStatus();
    }
    return null;
  }

  public abstract <K, V> KeyValueState<K, V> getKeyValueState(
      KeyValueStateDescriptor<K, V> stateDescriptor);

  public abstract <K, V> KeyValueState<K, V> getNonKeyedKeyValueState(
      KeyValueStateDescriptor<K, V> stateDescriptor);

  public abstract <K, UK, UV> KeyMapState<K, UK, UV> getKeyMapState(
      KeyMapStateDescriptor<K, UK, UV> stateDescriptor);

  public abstract <V> ValueState<V> getValueState(ValueStateDescriptor<V> stateDescriptor);

  public abstract <V> ValueState<V> getNonKeyedValueState(ValueStateDescriptor<V> stateDescriptor);

  public abstract Map<String, State> getStateMap();
}