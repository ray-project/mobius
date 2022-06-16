package io.ray.streaming.state.keystate.state;

/**
 * These kinds of states are not related to the data's key.
 */
public interface NonKeyedAble {

    /**
     * Get the unique state id.
     *
     * @return the state id
     */
    default int getUniqueStateKey() {
        return -1;
    }
}
