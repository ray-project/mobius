package io.ray.streaming.state.typeinfo.serializer.basic;

import io.ray.state.memory.DataInputView;
import io.ray.state.memory.DataOutputView;
import io.ray.state.typeinfo.serializer.TypeSerializer;
import java.io.IOException;

/**
 * Type serializer for short.
 */
public class ShortSerializer extends TypeSerializer<Short> {

  public static final ShortSerializer INSTANCE = new ShortSerializer();

  private static final Short DEFAULT_VALUE = (short) 0;

  @Override
  public void serialize(Short record, DataOutputView outputView) throws IOException {
    outputView.writeShort(record);
  }

  @Override
  public Short deserialize(DataInputView inputView) throws IOException {
    return inputView.readShort();
  }

  @Override
  public TypeSerializer<Short> duplicate() {
    return this;
  }

  @Override
  public Short createInstance() {
    return DEFAULT_VALUE;
  }
}
