package io.ray.streaming.state.typeinfo.serializer.basic;

import io.ray.state.memory.DataInputView;
import io.ray.state.memory.DataOutputView;
import io.ray.state.typeinfo.serializer.TypeSerializer;
import java.io.IOException;

/**
 * Type serializer for double.
 */
public class DoubleSerializer extends TypeSerializer<Double> {

  public static final DoubleSerializer INSTANCE = new DoubleSerializer();

  private static final Double DEFAULT_VALUE = 0.0;

  @Override
  public void serialize(Double record, DataOutputView outputView) throws IOException {
    outputView.writeDouble(record);
  }

  @Override
  public Double deserialize(DataInputView inputView) throws IOException {
    return inputView.readDouble();
  }

  @Override
  public TypeSerializer<Double> duplicate() {
    return this;
  }

  @Override
  public Double createInstance() {
    return DEFAULT_VALUE;
  }
}
