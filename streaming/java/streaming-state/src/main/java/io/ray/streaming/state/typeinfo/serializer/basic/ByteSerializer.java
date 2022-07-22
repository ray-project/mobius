package io.ray.streaming.state.typeinfo.serializer.basic;

import io.ray.streaming.state.buffer.DataInputView;
import io.ray.streaming.state.buffer.DataOutputView;
import io.ray.streaming.state.typeinfo.serializer.TypeSerializer;
import java.io.IOException;

/** Type serializer for byte. */
public class ByteSerializer extends TypeSerializer<Byte> {

  public static final ByteSerializer INSTANCE = new ByteSerializer();

  private static final Byte DEFAULT_VALUE = (byte) 0;

  @Override
  public void serialize(Byte record, DataOutputView outputView) throws IOException {
    outputView.writeByte(record);
  }

  @Override
  public Byte deserialize(DataInputView inputView) throws IOException {
    return inputView.readByte();
  }

  // basic type
  @Override
  public TypeSerializer<Byte> duplicate() {
    return this;
  }

  @Override
  public Byte createInstance() {
    return DEFAULT_VALUE;
  }
}
