package io.ray.streaming.state.buffer;

import java.lang.reflect.Field;
import sun.misc.Unsafe;

/**
 * The {@link Unsafe} can be used to perform native buffer accesses.
 */
public class UnsafeUtil {

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
}
