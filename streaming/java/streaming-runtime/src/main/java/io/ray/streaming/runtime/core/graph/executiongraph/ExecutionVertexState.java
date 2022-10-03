package io.ray.streaming.runtime.core.graph.executiongraph;

import java.io.Serializable;

/** execution vertex state enum */
public enum ExecutionVertexState implements Serializable {

  /** The added worker. */
  TO_ADD(0, "TO_ADD"),

  /** Other workers belong to the operator if operator add workers. */
  TO_ADD_RELATED(1, "TO_ADD_RELATED"),

  /** The deleted workers. */
  TO_DEL(2, "TO_DEL"),

  /** Other workers belong to the operator if operator delete workers. */
  TO_DEL_RELATED(3, "TO_DEL_RELATED"),

  /** Related workers.(Sub source and sink) */
  TO_UPDATE(4, "TO_UPDATE"),

  /** Unchanged workers. */
  RUNNING(5, "RUNNING"),

  /** Unknown status. */
  UNKNOWN(6, "UNKNOWN");

  public final int code;
  public final String msg;

  ExecutionVertexState(int code, String msg) {
    this.code = code;
    this.msg = msg;
  }
}
