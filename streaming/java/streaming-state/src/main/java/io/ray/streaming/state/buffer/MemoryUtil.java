package io.ray.streaming.state.buffer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import sun.misc.Unsafe;

public class MemoryUtil {

  public static final Unsafe UNSAFE;

  static {
    try {
      Field field = Unsafe.class.getDeclaredField("theUnsafe");
      field.setAccessible(true);
      UNSAFE = (Unsafe) field.get(null);
    } catch (Exception e) {
      throw new AssertionError(e);
    }
  }

  public static long getByteBufferAddress(ByteBuffer buffer) {
    checkNotNull(buffer, "buffer is null.");
    checkArgument(buffer.isDirect(), "buffer must DirectByteBuffer");

    // reflect get address
    final long bufferAddressFieldOffset = getClassFieldOffset(Buffer.class, "address");
    return UNSAFE.getLong(buffer, bufferAddressFieldOffset);
  }

  private static long getClassFieldOffset(Class<?> clazz, String fieldName) {
    try {
      return UNSAFE.objectFieldOffset(clazz.getDeclaredField(fieldName));
    } catch (Exception e) {
      throw new RuntimeException("Could not get field " + fieldName + " offset in class " + clazz);
    }
  }
}
