package io.ray.streaming.state.store.backend;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;

/** Store backend abstract interface. */
public interface StateBackend extends Serializable {

  /** Initialize the store backend system. */
  void init();

  /**
   * Perform snapshot asynchronously, the call will not block the caller.
   *
   * @param snapshotId snapshot id
   * @return future handler
   */
  CompletableFuture<Boolean> snapshot(long snapshotId);

  /**
   * The caller has failover and needs to be restored to the previous snapshot state.
   *
   * @param snapshotId restore checkpoint id.
   */
  void rollbackSnapshot(long snapshotId);

  /**
   * Clear the data of the specified snapshot.
   *
   * @param snapshotId Id of the snapshot to be deleted.
   */
  boolean deleteSnapshot(long snapshotId);

  void close();
}
