package io.ray.streaming.state.buffer;

import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.nio.ByteOrder;
import sun.misc.Unsafe;

/** TODO This class is temporary and will be replaced by buffer manager. */
public class DataInputView implements DataInput {

  private static final Unsafe UNSAFE = UnsafeUtil.UNSAFE;
  private static final long BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
  private static final boolean LITTLE_ENDIAN = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;

  private byte[] buffer;
  private int end;
  private int position;

  public DataInputView() {
    setBuffer(new byte[0]);
  }

  public void setBuffer(byte[] buffer) {
    setBuffer(buffer, 0, buffer.length);
  }

  public void setBuffer(byte[] buffer, int start, int len) {
    this.buffer = buffer;
    this.position = start;
    this.end = start + len;
  }

  @Override
  public byte readByte() throws IOException {
    if (this.position >= 0 && this.position < this.end) {
      return this.buffer[this.position++];
    } else {
      throw new EOFException();
    }
  }

  @Override
  public char readChar() throws IOException {
    if (this.position >= 0 && this.position < this.end - 1) {
      return (char)
          (((this.buffer[this.position++] & 0xff)) << 8 | (this.buffer[this.position++] & 0xff));
    } else {
      throw new EOFException();
    }
  }

  @Override
  public short readShort() throws IOException {
    if (this.position >= 0 && this.position < this.end - 1) {
      return (short)
          (((this.buffer[this.position++] & 0xff) << 8) | (this.buffer[this.position++] & 0xff));
    } else {
      throw new EOFException();
    }
  }

  @Override
  public int readInt() throws IOException {
    if (this.position >= 0 && this.position < this.end - 3) {
      int value = UNSAFE.getInt(this.buffer, BASE_OFFSET + this.position);
      if (LITTLE_ENDIAN) {
        value = Integer.reverseBytes(value);
      }
      this.position += 4;
      return value;
    } else {
      throw new EOFException();
    }
  }

  @Override
  public long readLong() throws IOException {
    if (this.position >= 0 && this.position < this.end - 7) {
      long value = UNSAFE.getLong(this.buffer, BASE_OFFSET + this.position);
      if (LITTLE_ENDIAN) {
        value = Long.reverseBytes(value);
      }
      this.position += 8;
      return value;
    } else {
      throw new EOFException();
    }
  }

  @Override
  public float readFloat() throws IOException {
    return Float.intBitsToFloat(readInt());
  }

  @Override
  public double readDouble() throws IOException {
    return Double.longBitsToDouble(readLong());
  }

  @Override
  public boolean readBoolean() throws IOException {
    return readByte() != 0;
  }

  @Override
  public void readFully(byte[] b) throws IOException {
    readFully(b, 0, b.length);
  }

  @Override
  public void readFully(byte[] b, int off, int len) throws IOException {
    if (len >= 0) {
      if (off + len <= b.length) {
        if (this.position >= 0 && this.position + len <= this.end) {
          System.arraycopy(this.buffer, position, b, off, len);
          this.position += len;
        } else {
          throw new EOFException();
        }
      } else {
        throw new ArrayIndexOutOfBoundsException();
      }
    } else {
      throw new IllegalArgumentException("Len may not be negative");
    }
  }

  @Override
  public int skipBytes(int n) throws IOException {
    if (this.position <= this.end - n) {
      this.position += n;
      return n;
    } else {
      n = this.end - this.position;
      this.position = this.end;
      return n;
    }
  }

  @Override
  public int readUnsignedByte() throws IOException {
    if (this.position < this.end) {
      return (this.buffer[this.position++] & 0xff);
    } else {
      throw new EOFException();
    }
  }

  @Override
  public int readUnsignedShort() throws IOException {
    if (this.position < this.end - 1) {
      return ((this.buffer[this.position++] & 0xff) << 8) | (this.buffer[this.position++] & 0xff);
    } else {
      throw new EOFException();
    }
  }

  @Override
  public String readLine() throws IOException {
    if (this.position < this.end) {
      StringBuilder sb = new StringBuilder();
      char c = (char) readUnsignedByte();
      while (this.position < this.end && c != '\n') {
        sb.append(c);
        c = (char) readUnsignedByte();
      }

      int len = sb.length();
      if (len > 0 && sb.charAt(len - 1) == '\r') {
        sb.setLength(len - 1);
      }
      String s = sb.toString();
      sb.setLength(0);
      return s;
    } else {
      return null;
    }
  }

  @Override
  public String readUTF() throws IOException {
    // read length
    int utfLen = readUnsignedShort();
    byte[] byteArray = new byte[utfLen];
    char[] charArray = new char[utfLen];

    int c;
    int char2;
    int char3;
    int count = 0;
    int charArrayCount = 0;

    readFully(byteArray, 0, utfLen);

    while (count < utfLen) {
      c = (int) byteArray[count] & 0xff;
      if (c > 127) {
        break;
      }
      count++;
      charArray[charArrayCount++] = (char) c;
    }

    while (count < utfLen) {
      c = (int) byteArray[count] & 0xff;
      switch (c >> 4) {
        case 0:
        case 1:
        case 2:
        case 3:
        case 4:
        case 5:
        case 6:
        case 7:
          /* 0xxxxxxx */
          count++;
          charArray[charArrayCount++] = (char) c;
          break;
        case 12:
        case 13:
          /* 110x xxxx 10xx xxxx */
          count += 2;
          if (count > utfLen) {
            throw new UTFDataFormatException("malformed input: partial character at end");
          }
          char2 = (int) byteArray[count - 1];
          if ((char2 & 0xC0) != 0x80) {
            throw new UTFDataFormatException("malformed input around byte " + count);
          }
          charArray[charArrayCount++] = (char) (((c & 0x1F) << 6) | (char2 & 0x3F));
          break;
        case 14:
          /* 1110 xxxx 10xx xxxx 10xx xxxx */
          count += 3;
          if (count > utfLen) {
            throw new UTFDataFormatException("malformed input: partial character at end");
          }
          char2 = (int) byteArray[count - 2];
          char3 = (int) byteArray[count - 1];
          if (((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80)) {
            throw new UTFDataFormatException("malformed input around byte " + (count - 1));
          }
          charArray[charArrayCount++] =
              (char) (((c & 0x0F) << 12) | ((char2 & 0x3F) << 6) | (char3 & 0x3F));
          break;
        default:
          /* 10xx xxxx, 1111 xxxx */
          throw new UTFDataFormatException("malformed input around byte " + count);
      }
    }
    // The number of chars produced may be less than utflen
    return new String(charArray, 0, charArrayCount);
  }

  public int read(byte[] b, int off, int len) throws IOException {
    if (off < 0 || len < 0) {
      throw new IndexOutOfBoundsException(
          "Offset:" + off + " or len: " + len + " cannot be nagative");
    }

    if (b.length - off < len) {
      throw new IndexOutOfBoundsException(
          "Byte array does not provide enough space to store requested data.");
    }

    if (this.position >= this.end) {
      return -1;
    } else {
      int toRead = Math.min(this.end - this.position, len);
      System.arraycopy(this.buffer, this.position, b, off, toRead);
      this.position += toRead;

      return toRead;
    }
  }
}
