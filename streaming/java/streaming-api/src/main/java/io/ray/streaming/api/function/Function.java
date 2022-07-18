package io.ray.streaming.api.function;

import java.io.Serializable;

/** Interface of streaming functions. */
public interface Function extends Serializable {

  /**
   * Mark this checkpoint has been finished.
   *
   * @param checkpointId
   * @throws Exception
   */
  default void finish(long checkpointId) throws Exception {}

  /**
   * This method will be called periodically by framework, you should return a a serializable object
   * which represents function state, framework will help you to serialize this object, save it to
   * storage, and load it back when in fail-over through. {@link
   * Function#loadCheckpoint(Serializable)}.
   *
   * @return A serializable object which represents function state.
   */
  default Serializable saveCheckpoint() {
    return null;
  }

  /**
   * Save current checkpoint info using checkpoint id.
   *
   * @param checkpointId the checkpoint id
   * @throws Exception error
   */
  default void saveCheckpoint(long checkpointId) throws Exception {}

  /**
   * This method will be called by framework when a worker died and been restarted. We will pass the
   * last object you returned in {@link Function#saveCheckpoint()} when doing checkpoint, you are
   * responsible to load this object back to you function.
   *
   * @param checkpointObject the last object you returned in {@link Function#saveCheckpoint()}
   */
  default void loadCheckpoint(Serializable checkpointObject) {}

  /**
   * Delete user-defined checkpoint by checkpoint id.
   *
   * @param checkpointId
   * @throws Exception
   */
  default void deleteCheckpoint(long checkpointId) throws Exception {}

  /**
   * User defined command that need to be executed by the function.
   *
   * @param commandMessage the command message
   */
  default void forwardCommand(String commandMessage) {}

  /**
   * User defined latch for rescaling.
   *
   * @return is ready
   */
  default boolean isReadyRescaling() {
    return true;
  }

  /**
   * Tear-down method for the user function which called after the last call to the user function.
   */
  default void close() {}
}
