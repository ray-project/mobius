package io.ray.streaming.state.typeinfo.serializer.basic;

import io.ray.streaming.state.buffer.DataInputView;
import io.ray.streaming.state.buffer.DataOutputView;
import io.ray.streaming.state.typeinfo.serializer.TypeSerializer;
import java.io.IOException;

/** Type serializer for float. */
public class FloatSerializer extends TypeSerializer<Float> {

  public static final FloatSerializer INSTANCE = new FloatSerializer();

  private static final Float DEFAULT_VALUE = 0.0f;

  @Override
  public void serialize(Float record, DataOutputView outputView) throws IOException {
    outputView.writeFloat(record);
  }

  @Override
  public Float deserialize(DataInputView inputView) throws IOException {
    return inputView.readFloat();
  }

  @Override
  public TypeSerializer<Float> duplicate() {
    return this;
  }

  @Override
  public Float createInstance() {
    return DEFAULT_VALUE;
  }
}
