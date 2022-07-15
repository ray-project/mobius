package io.ray.streaming.state.buffer;

import java.nio.ByteBuffer;

public class OffHeapMemorySegment extends MemorySegment {

  private ByteBuffer offHeapBuffer;

  public OffHeapMemorySegment(ByteBuffer buffer) {
    super(MemoryUtil.getByteBufferAddress(buffer), buffer.capacity());
    this.offHeapBuffer = buffer;
  }

  @Override
  public void free() {
    super.free();
    this.offHeapBuffer = null;
  }
}
