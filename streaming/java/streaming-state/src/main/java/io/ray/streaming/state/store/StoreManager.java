package io.ray.streaming.state.store;

import io.ray.streaming.state.RollbackSnapshotResult;
import io.ray.streaming.state.SnapshotResult;
import io.ray.streaming.state.keystate.desc.KeyMapStateDescriptor;
import io.ray.streaming.state.keystate.desc.KeyValueStateDescriptor;
import io.ray.streaming.state.keystate.desc.ValueStateDescriptor;
import io.ray.streaming.state.keystate.state.KeyMapState;
import io.ray.streaming.state.keystate.state.KeyValueState;
import io.ray.streaming.state.keystate.state.ValueState;
import java.util.concurrent.CompletableFuture;

public interface StoreManager extends Store {

  /**
   * Build a key-value type state storage backend.
   */
  <K, V> KeyValueState<K, V> buildKeyValueStore(KeyValueStateDescriptor<K, V> descriptor);

  /**
   * Build a key-value type(nonKeyed) state storage backend.
   */
  <K, V> KeyValueState<K, V> buildNonKeyedKeyValueStore(KeyValueStateDescriptor<K, V> descriptor);

  /**
   * Build a key-map type state storage backend.
   */
  <K, UK, UV> KeyMapState<K, UK, UV> buildKeyMapStore(KeyMapStateDescriptor<K, UK, UV> descriptor);

  /**
   * Build a value type state storage backend.
   */
  <V> ValueState<V> buildValueStore(ValueStateDescriptor<V> descriptor);

  /**
   * Build a value type(nonKeyed) state storage backend.
   */
  <V> ValueState<V> buildNonKeyedValueStore(ValueStateDescriptor<V> descriptor);

  /**
   * Generate a snapshot of the current state, the operation is executed asynchronously and return a
   * {@link CompletableFuture}.
   */
  CompletableFuture<SnapshotResult> snapshot(long snapshotId);

  /**
   * Rollback to the specified state snapshot.
   */
  CompletableFuture<RollbackSnapshotResult> rollbackSnapshot(long snapshotId);

  /**
   * Delete the specified snapshot data.
   */
  boolean deleteSnapshot(long snapshotId);

  void setCurrentKey(Object key);

  void close();

  default StoreStatus getStoreStatus() {
    return null;
  }
}