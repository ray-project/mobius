package io.ray.streaming.state.buffer;

import com.google.common.annotations.VisibleForTesting;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.nio.ByteOrder;
import java.util.Arrays;
import sun.misc.Unsafe;

/**
 * TODO
 * This class is temporary and will be replaced by buffer manager.
 */
public class DataOutputView implements DataOutput {

  private static final Unsafe UNSAFE = UnsafeUtil.UNSAFE;
  private static final long BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
  private static final boolean LITTLE_ENDIAN = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;

  private byte[] buffer;

  private int position;

  public DataOutputView() {
    this(128);
  }

  public DataOutputView(int size) {
    if (size < 1) {
      throw new IllegalArgumentException();
    }
    this.buffer = new byte[size];
  }

  @Override
  public void write(int b) throws IOException {
    if (this.position >= this.buffer.length) {
      resize(1);
    }
    //Deal with negative number and ensure that the binary high bits are filled with 0.
    this.buffer[this.position++] = (byte) (b & 0xff);
  }

  @Override
  public void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (len < 0 || off > b.length - len) {
      throw new ArrayIndexOutOfBoundsException();
    }
    if (this.position > this.buffer.length - len) {
      resize(len);
    }
    System.arraycopy(b, off, this.buffer, this.position, len);
    this.position += len;
  }

  @Override
  public void writeByte(int v) throws IOException {
    write(v);
  }

  @Override
  public void writeBoolean(boolean v) throws IOException {
    if (v) {
      write(1);
    } else {
      write(0);
    }
  }

  @Override
  public void writeShort(int v) throws IOException {
    if (this.position >= this.buffer.length - 1) {
      //shot is 2 bytes
      resize(2);
    }

    //high 8 bit
    this.buffer[this.position++] = (byte) ((v >>> 8) & 0xff);
    //low 8 bit
    this.buffer[this.position++] = (byte) (v & 0xff);
  }

  @Override
  public void writeChar(int v) throws IOException {
    if (this.position >= this.buffer.length - 1) {
      //char is 2 bytes
      resize(2);
    }
    this.buffer[this.position++] = (byte) (v >> 8);
    this.buffer[this.position++] = (byte) v;
  }

  @Override
  public void writeChars(String s) throws IOException {
    int len = s.length();
    if (this.position >= this.buffer.length - 2 * len) {
      resize(2 * len);
    }
    for (int i = 0; i < len; i++) {
      writeChar(s.charAt(i));
    }
  }

  @Override
  public void writeInt(int v) throws IOException {
    if (this.position >= this.buffer.length - 3) {
      //int is 4 bytes.
      resize(4);
    }
    if (LITTLE_ENDIAN) {
      v = Integer.reverseBytes(v);
    }
    UNSAFE.putInt(this.buffer, BASE_OFFSET + this.position, v);
    this.position += 4;
  }

  @Override
  public void writeLong(long v) throws IOException {
    if (this.position >= this.buffer.length - 7) {
      resize(8);
    }
    if (LITTLE_ENDIAN) {
      v = Long.reverseBytes(v);
    }
    UNSAFE.putLong(this.buffer, BASE_OFFSET + this.position, v);
    this.position += 8;
  }

  @Override
  public void writeFloat(float v) throws IOException {
    writeInt(Float.floatToIntBits(v));
  }

  @Override
  public void writeDouble(double v) throws IOException {
    writeLong(Double.doubleToLongBits(v));
  }

  @Override
  public void writeBytes(String s) throws IOException {
    int len = s.length();
    if (this.position >= this.buffer.length - len) {
      resize(len);
    }

    for (int i = 0; i < len; i++) {
      writeByte(s.charAt(i));
    }
  }

  @Override
  public void writeUTF(String s) throws IOException {
    int len = s.length();
    int utflen = 0;
    int c;

    for (int i = 0; i < len; i++) {
      c = s.charAt(i);
      if ((c >= 0x0001) && (c <= 0x007F)) {
        utflen++;
      } else if (c > 0x07FF) {
        utflen += 3;
      } else {
        utflen += 2;
      }
    }

    if (utflen > 65535) {
      throw new UTFDataFormatException("Encoded string is too long: " + utflen);
    }
    else if (this.position > this.buffer.length - utflen - 2) {
      resize(utflen + 2);
    }

    byte[] byteArray = this.buffer;
    int count = this.position;

    //write length
    byteArray[count++] = (byte) ((utflen >>> 8) & 0xFF);
    byteArray[count++] = (byte) (utflen & 0xFF);

    int i;
    for (i = 0; i < len; i++) {
      c = s.charAt(i);
      if (!((c >= 0x0001) && (c <= 0x007F))) {
        break;
      }
      byteArray[count++] = (byte) c;
    }

    for (; i < len; i++) {
      c = s.charAt(i);
      if ((c >= 0x0001) && (c <= 0x007F)) {
        byteArray[count++] = (byte) c;

      } else if (c > 0x07FF) {
        byteArray[count++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
        byteArray[count++] = (byte) (0x80 | ((c >>  6) & 0x3F));
        byteArray[count++] = (byte) (0x80 | (c & 0x3F));
      } else {
        byteArray[count++] = (byte) (0xC0 | ((c >> 6) & 0x1F));
        byteArray[count++] = (byte) (0x80 | (c & 0x3F));
      }
    }

    this.position = count;
  }

  public void clear() {
    this.position = 0;
  }

  public byte[] getCopyOfBuffer() {
    return Arrays.copyOf(buffer, position);
  }

  private void resize(int minCapacityAdd) throws IOException {
    //if the expansion size is less than 2 times the original size, the default expansion is 2 times.
    int newLen = Math.max(this.buffer.length * 2, this.buffer.length + minCapacityAdd);
    byte[] newBuffer;
    try {
      newBuffer = new byte[newLen];
    } catch (OutOfMemoryError e) {
      if (newLen > (this.buffer.length + minCapacityAdd)) {
        newLen = this.buffer.length + minCapacityAdd;
        try {
          newBuffer = new byte[newLen];
        } catch (OutOfMemoryError ee) {
          throw new IOException("Failed to serialize element. Serialized size(" + newLen + "bytes) exceeds available JVM heap space.", ee);
        }
      } else {
        throw new IOException("Failed to serialize element. Serialized size(" + newLen + "bytes) exceeds available JVM heap space.", e);
      }
    }
    //copy to new array.
    System.arraycopy(this.buffer, 0, newBuffer, 0, this.position);
    this.buffer = newBuffer;
  }

  @VisibleForTesting
  public int getPosition() {
    return position;
  }
}
