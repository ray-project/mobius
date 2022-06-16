package io.ray.streaming.state.keystate.state;

public interface HiddenKeyState extends KeyedState {

  /**
   * Get the key.
   *
   * @return key for value state
   */
  Object getCurrentKey();

  /**
   * Set the specified key.
   *
   * @param currentKey key for value state
   */
  void setCurrentKey(Object currentKey);
}
