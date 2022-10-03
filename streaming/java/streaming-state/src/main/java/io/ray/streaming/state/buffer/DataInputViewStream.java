package io.ray.streaming.state.buffer;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

public class DataInputViewStream extends InputStream {

  private DataInputView inputView;

  public DataInputViewStream(DataInputView inputView) {
    this.inputView = inputView;
  }

  @Override
  public int read() throws IOException {
    try {
      return inputView.readUnsignedByte();
    } catch (EOFException e) {
      return -1;
    }
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return inputView.read(b, off, len);
  }

  @Override
  public long skip(long n) throws IOException {
    long toSkipRemaining = n;
    while (toSkipRemaining > Integer.MAX_VALUE) {
      int skippedBytes = inputView.skipBytes(Integer.MAX_VALUE);

      if (skippedBytes == 0) {
        return n - toSkipRemaining;
      }

      toSkipRemaining -= skippedBytes;
    }
    return n - (toSkipRemaining - inputView.skipBytes((int) toSkipRemaining));
  }
}
