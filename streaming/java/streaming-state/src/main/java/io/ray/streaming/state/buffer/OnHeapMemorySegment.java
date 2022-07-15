package io.ray.streaming.state.buffer;

/**
 * On-heap buffer segment, using byte array to store data.
 */
public class OnHeapMemorySegment extends MemorySegment {

  private byte[] heapMemory;

  public OnHeapMemorySegment(byte[] memory) {
    super(memory);
    this.heapMemory = memory;
  }

  @Override
  public void free() {
    super.free();
    this.heapMemory = null;
  }
}
