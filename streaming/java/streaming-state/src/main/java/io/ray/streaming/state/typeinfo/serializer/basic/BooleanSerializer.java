package io.ray.streaming.state.typeinfo.serializer.basic;

import io.ray.streaming.state.buffer.DataInputView;
import io.ray.streaming.state.buffer.DataOutputView;
import io.ray.streaming.state.typeinfo.serializer.TypeSerializer;
import java.io.IOException;

/** Type serializer for boolean. */
public class BooleanSerializer extends TypeSerializer<Boolean> {

  public static final BooleanSerializer INSTANCE = new BooleanSerializer();

  private static final Boolean DEFAULT_VALUE = Boolean.FALSE;

  @Override
  public void serialize(Boolean record, DataOutputView outputView) throws IOException {
    outputView.writeBoolean(record);
  }

  @Override
  public Boolean deserialize(DataInputView inputView) throws IOException {
    return inputView.readBoolean();
  }

  @Override
  public TypeSerializer<Boolean> duplicate() {
    return this;
  }

  @Override
  public Boolean createInstance() {
    return DEFAULT_VALUE;
  }
}
