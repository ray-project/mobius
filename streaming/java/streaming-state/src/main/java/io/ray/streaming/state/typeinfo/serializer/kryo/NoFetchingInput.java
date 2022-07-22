package io.ray.streaming.state.typeinfo.serializer.kryo;

import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

public class NoFetchingInput extends Input {
  public NoFetchingInput(InputStream inputStream) {
    super(inputStream, 8);
  }

  @Override
  public boolean eof() {
    throw new UnsupportedOperationException("NoFetchingInput does not support EOF.");
  }

  @Override
  public int read() throws KryoException {
    require(1);
    return buffer[position++] & 0xFF;
  }

  @Override
  public boolean canReadInt() throws KryoException {
    throw new UnsupportedOperationException("NoFetchingInput cannot prefetch data.");
  }

  @Override
  public boolean canReadLong() throws KryoException {
    throw new UnsupportedOperationException("NoFetchingInput cannot prefetch data.");
  }

  @Override
  protected int require(int required) throws KryoException {
    if (required > capacity) {
      throw new KryoException(
          "Buffer too small: capacity: " + capacity + ", " + "required: " + required);
    }

    position = 0;
    int bytesRead = 0;
    int count;
    while (true) {
      count = fill(buffer, bytesRead, required - bytesRead);

      if (count == -1) {
        throw new KryoException(new EOFException("No more bytes left."));
      }

      bytesRead += count;
      if (bytesRead == required) {
        break;
      }
    }
    limit = required;
    return required;
  }

  @Override
  public int read(byte[] bytes, int offset, int count) throws KryoException {
    if (bytes == null) {
      throw new IllegalArgumentException("bytes cannot be null.");
    }

    try {
      return inputStream.read(bytes, offset, count);
    } catch (IOException ex) {
      throw new KryoException(ex);
    }
  }

  @Override
  public void skip(int count) throws KryoException {
    try {
      inputStream.skip(count);
    } catch (IOException ex) {
      throw new KryoException(ex);
    }
  }

  @Override
  public void readBytes(byte[] bytes, int offset, int count) throws KryoException {
    if (bytes == null) {
      throw new IllegalArgumentException("bytes cannot be null.");
    }

    try {
      int bytesRead = 0;
      int c;

      while (true) {
        c = inputStream.read(bytes, offset + bytesRead, count - bytesRead);

        if (c == -1) {
          throw new KryoException(new EOFException("No more bytes left."));
        }

        bytesRead += c;

        if (bytesRead == count) {
          break;
        }
      }
    } catch (IOException ex) {
      throw new KryoException(ex);
    }
  }
}
