package io.ray.streaming.state.buffer;

/** Apply for Native buffer type entity class. */
public interface NativeMemoryInfo {

  /** Request total buffer size. */
  long getTotalMemorySize();

  /** Currently used buffer. */
  long getUsedMemorySize();
}
