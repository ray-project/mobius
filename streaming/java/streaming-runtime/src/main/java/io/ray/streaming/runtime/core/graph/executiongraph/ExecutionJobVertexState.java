package io.ray.streaming.runtime.core.graph.executiongraph;

import java.io.Serializable;

public enum ExecutionJobVertexState implements Serializable {

  /**
   * Normal state(unchanged).
   */
  NORMAL("normal"),

  /**
   * Changed state.
   */
  CHANGED("changed");

  private String value;

  ExecutionJobVertexState(String value) {
    this.value = value;
  }

}
