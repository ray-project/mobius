package io.ray.streaming.state.buffer;

import java.io.IOException;
import java.io.OutputStream;

public class DataOutputViewStream extends OutputStream {

  public DataOutputView outputView;

  public DataOutputViewStream(DataOutputView outputView) {
    this.outputView = outputView;
  }

  @Override
  public void write(int b) throws IOException {
    outputView.writeByte(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    outputView.write(b, off, len);
  }
}
