package io.ray.streaming.state.buffer;

import sun.misc.Unsafe;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * The smallest unit of buffer manager. Including on-heap buffer segment and off-heap buffer segment.
 */
public abstract class MemorySegment {

  protected static final Unsafe UNSAFE = UnsafeUtil.UNSAFE;
  protected static final long BYTE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);

  /**
   * If the segment is stored in the on-heap, the byte array is used to store the data in the heap.
   */
  protected final byte[] heapMemory;

  /**
   * If the segment is on-heap, address represents the index of the byte array. If the segment is
   * off-heap, address represents the absolute buffer address on off-heap.
   */
  protected long address;

  protected long addressLimit;

  /**
   * The size in bytes of the buffer segment.
   */
  protected final int size;

  /**
   * On-heap buffer segment.
   */
  public MemorySegment(byte[] heapMemory) {
    checkNotNull(heapMemory);

    this.heapMemory = heapMemory;
    this.address = BYTE_ARRAY_BASE_OFFSET;
    this.size = heapMemory.length;
    this.addressLimit = this.address + this.size;
  }

  /**
   * Off-heap buffer segment.
   */
  public MemorySegment(long offHeapAddress, int size) {
    if (offHeapAddress <= 0 || size <= 0) {
      throw new IllegalArgumentException("Off-heap address or size  must non negative.");
    }

    this.heapMemory = null;
    this.address = offHeapAddress;
    this.size = size;
    this.addressLimit = this.address + size;
  }

  public void put(int index, byte b) {
    final long pos = address + index;
    if (index >= 0 && pos <= addressLimit - 1) {
      UNSAFE.putByte(heapMemory, pos, b);
    } else {
      throw new IndexOutOfBoundsException();
    }
  }

  public  void put(int index, byte[] src) {
    put(index, src, 0, src.length);
  }

  public void put(int index, byte[] src, int offset, int length) {
    final long pos = address + index;
    if (index >= 0 && pos <= addressLimit - length) {
      final long arrayAddress = BYTE_ARRAY_BASE_OFFSET + offset;
      UNSAFE.copyMemory(src, arrayAddress, heapMemory, pos, length);
    } else {
      throw new IndexOutOfBoundsException();
    }
  }

  public byte get(int index) {
    final long pos = address + index;
    if (index >= 0 && pos <= addressLimit - 1) {
      return UNSAFE.getByte(heapMemory, pos);
    } else {
      throw new IndexOutOfBoundsException();
    }
  }

  public void get(int index, byte[] dst) {
    get(index, dst, 0, dst.length);
  }

  public void get(int index, byte[] dst, int offset, int length) {
    final long pos = address + index;
    if (index >=0 && pos <= addressLimit - length) {
      final long arrayAddress = BYTE_ARRAY_BASE_OFFSET + offset;
      UNSAFE.copyMemory(heapMemory, pos, dst, arrayAddress, length);
    } else {
      throw new IndexOutOfBoundsException();
    }
  }

  public void putBoolean(int index, boolean value) {
    put(index, (byte) (value ? 1 : 0));
  }

  public boolean getBoolean(int index) {
    return get(index) != 0;
  }

  public void putChar(int index, char value) {
    final long pos = address + index;
    if (index >=0 && pos <= this.addressLimit - 2) {
      UNSAFE.putChar(heapMemory, pos, value);
    } else {
      throw new IndexOutOfBoundsException();
    }
  }

  public char getChar(int index) {
    final long pos = address + index;
    if (index >=0 && pos <= addressLimit - 2) {
      return UNSAFE.getChar(heapMemory, pos);
    } else {
      throw new IndexOutOfBoundsException();
    }
  }

  public void putShort(int index, short value) {
    final long pos = address + index;
    if (index >= 0 && pos <= addressLimit - 2) {
      UNSAFE.putShort(heapMemory, pos, value);
    } else {
      throw new IndexOutOfBoundsException();
    }
  }

  public short getShort(int index) {
    final long pos = address + index;
    if (index >= 0 && pos <= addressLimit - 2) {
      return UNSAFE.getShort(heapMemory, pos);
    } else {
      throw new IndexOutOfBoundsException();
    }
  }

  public void putInt(int index, int value) {
    final long pos = address + index;
    if (index >=0 && pos <= addressLimit - 4) {
      UNSAFE.putInt(heapMemory, pos, value);
    } else {
      throw new IndexOutOfBoundsException();
    }
  }

  public int getInt(int index) {
    final long pos = address + index;
    if (index >= 0 && pos < addressLimit - 4) {
      return UNSAFE.getInt(heapMemory, pos);
    } else {
      throw new IndexOutOfBoundsException();
    }
  }

  public void putLong(int index, long value) {
    final long pos = address + index;
    if (index >= 0 && pos <= addressLimit - 8) {
      UNSAFE.putLong(heapMemory, pos, value);
    } else {
      throw new IndexOutOfBoundsException();
    }
  }

  public long getLong(int index) {
    final long pos = address + index;
    if (index >=0 && pos <= addressLimit - 8) {
      return UNSAFE.getLong(heapMemory, pos);
    } else {
      throw new IndexOutOfBoundsException();
    }
  }

  public void putFloat(int index, float value) {
    putInt(index, Float.floatToRawIntBits(value));
  }

  public float getFloat(int index)  {
    return Float.intBitsToFloat(getInt(index));
  }

  public void putDouble(int index, double value) {
    putLong(index, Double.doubleToRawLongBits(value));
  }

  public double getDouble(int index) {
    return Double.longBitsToDouble(getLong(index));
  }

  public void free() {
    address = addressLimit + 1;
  }
}
