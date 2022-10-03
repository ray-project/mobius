package io.ray.streaming.state.store;

import io.ray.streaming.state.RollbackSnapshotResult;
import io.ray.streaming.state.SnapshotResult;
import io.ray.streaming.state.api.desc.ListStateDescriptor;
import io.ray.streaming.state.api.desc.MapStateDescriptor;
import io.ray.streaming.state.api.desc.ValueStateDescriptor;
import io.ray.streaming.state.api.state.ListState;
import io.ray.streaming.state.api.state.MapState;
import io.ray.streaming.state.api.state.ValueState;
import java.util.concurrent.CompletableFuture;

public interface StoreManager extends Store {

  /** Build a key-value type state storage backend. */
  <K, V> MapState<K, V> buildMapStore(MapStateDescriptor<K, V> descriptor);

  /** Build a key-value type(nonKeyed) state storage backend. */
  <K, V> MapState<K, V> buildNonKeyedMapStore(MapStateDescriptor<K, V> descriptor);

  /** Build a value type state storage backend. */
  <V> ValueState<V> buildValueStore(ValueStateDescriptor<V> descriptor);

  /** Build a value type(nonKeyed) state storage backend. */
  <V> ValueState<V> buildNonKeyedValueStore(ValueStateDescriptor<V> descriptor);

  /** Build a list type state storage backend. */
  default <V> ListState<V> buildNonKeyedListStore(ListStateDescriptor<V> descriptor) {
    throw new UnsupportedOperationException();
  }

  /**
   * Generate a snapshot of the current state, the operation is executed asynchronously and return a
   * {@link CompletableFuture}.
   */
  CompletableFuture<SnapshotResult> snapshot(long snapshotId);

  /** Rollback to the specified state snapshot. */
  CompletableFuture<RollbackSnapshotResult> rollbackSnapshot(long snapshotId);

  /** Delete the specified snapshot data. */
  boolean deleteSnapshot(long snapshotId);

  void setCurrentKey(Object key);

  void close();

  default StoreStatus getStoreStatus() {
    return null;
  }
}
