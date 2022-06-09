package io.ray.streaming.runtime.worker.context;

import io.ray.state.manager.StateManager;
import io.ray.streaming.api.context.RuntimeContext;

/**
 * Internal context including some operation.
 */
public interface InternalRuntimeContext extends RuntimeContext {

  /**
   * Update checkpoint id for stream workload's context.
   */
  void updateCheckpointId(long checkpointId);

  /**
   * Return the state manager.
   * @return state manager
   */
  StateManager getStateManager();

  /**
   * Set the state manager.
   * @param stateManager state manager
   */
  void setStateManager(StateManager stateManager);
}
