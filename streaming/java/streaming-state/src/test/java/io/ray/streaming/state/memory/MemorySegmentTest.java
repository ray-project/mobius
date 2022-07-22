package io.ray.streaming.state.memory;

import io.ray.streaming.state.buffer.MemorySegment;
import io.ray.streaming.state.buffer.OffHeapMemorySegment;
import io.ray.streaming.state.buffer.OnHeapMemorySegment;
import java.nio.ByteBuffer;
import org.testng.Assert;
import org.testng.annotations.Test;

public class MemorySegmentTest {

  @Test
  public void testOnHeapMemorySegment() {
    MemorySegment onHeapMemorySegment = new OnHeapMemorySegment(new byte[1024 * 1024]);
    testMemorySegment(onHeapMemorySegment);
  }

  @Test
  public void testOffHeapMemorySegment() {
    MemorySegment offHeapMemorySegment =
        new OffHeapMemorySegment(ByteBuffer.allocateDirect(1024 * 1024));
    testMemorySegment(offHeapMemorySegment);
  }

  private void testMemorySegment(MemorySegment memorySegment) {

    memorySegment.put(0, (byte) 12);
    Assert.assertEquals(memorySegment.get(0), (byte) 12);

    String testStr = "testStr";
    memorySegment.put(0, testStr.getBytes());
    byte[] readTestStrBytes = new byte[testStr.getBytes().length];
    memorySegment.get(0, readTestStrBytes);
    Assert.assertEquals(new String(readTestStrBytes), testStr);

    memorySegment.putShort(0, (short) 123);
    Assert.assertEquals(memorySegment.getShort(0), (short) 123);

    memorySegment.putInt(0, 123);
    Assert.assertEquals(memorySegment.getInt(0), 123);

    memorySegment.putLong(0, 123456789L);
    Assert.assertEquals(memorySegment.getLong(0), 123456789L);

    memorySegment.putFloat(0, 3.1415f);
    Assert.assertEquals(memorySegment.getFloat(0), 3.1415f);

    memorySegment.putDouble(0, 3.1415926);
    Assert.assertEquals(memorySegment.getDouble(0), 3.1415926);

    memorySegment.putBoolean(0, false);
    Assert.assertFalse(memorySegment.getBoolean(0));

    memorySegment.putChar(0, 'q');
    Assert.assertEquals(memorySegment.getChar(0), 'q');

    memorySegment.free();
    Assert.assertThrows(IndexOutOfBoundsException.class, () -> memorySegment.putInt(0, 123));
  }
}
