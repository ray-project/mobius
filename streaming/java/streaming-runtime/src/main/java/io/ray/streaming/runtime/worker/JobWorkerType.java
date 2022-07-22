package io.ray.streaming.runtime.worker;

public enum JobWorkerType {

  /** Normal java worker. */
  JAVA_WORKER,

  /** Normal python worker. */
  PYTHON_WORKER,

  /** History python worker.(Deprecated) */
  DYNAMIC_PY_WORKER
}
