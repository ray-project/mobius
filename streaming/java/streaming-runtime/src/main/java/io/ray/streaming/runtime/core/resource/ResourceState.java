package io.ray.streaming.runtime.core.resource;

/** Express state for bundle and placement group. */
public enum ResourceState {

  /** Resource is in used. */
  IN_USE(0, "IN_USE"),

  /** Content is empty, and the resource should be released. */
  TO_RELEASE(1, "TO_RELEASE"),

  /** Resource is not been managed. */
  UNKNOWN(-1, "UNKNOWN");

  public final int code;
  public final String msg;

  ResourceState(int code, String msg) {
    this.code = code;
    this.msg = msg;
  }
}
