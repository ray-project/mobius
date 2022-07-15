package io.ray.streaming.state.typeinfo.serializer.basic;

import io.ray.state.memory.DataInputView;
import io.ray.state.memory.DataOutputView;
import io.ray.state.typeinfo.serializer.TypeSerializer;
import java.io.IOException;

/**
 * Type serializer for int.
 */
public class IntegerSerializer extends TypeSerializer<Integer> {

  public static final IntegerSerializer INSTANCE = new IntegerSerializer();

  private static final Integer DEFAULT_VALUE = 0;

  @Override
  public void serialize(Integer record, DataOutputView outputView) throws IOException {
    outputView.writeInt(record);
  }

  @Override
  public Integer deserialize(DataInputView inputView) throws IOException {
    return inputView.readInt();
  }

  @Override
  public TypeSerializer<Integer> duplicate() {
    return this;
  }

  @Override
  public Integer createInstance() {
    return DEFAULT_VALUE;
  }
}
