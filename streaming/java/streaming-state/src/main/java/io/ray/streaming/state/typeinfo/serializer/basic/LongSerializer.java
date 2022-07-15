package io.ray.streaming.state.typeinfo.serializer.basic;

import io.ray.streaming.state.buffer.DataInputView;
import io.ray.streaming.state.buffer.DataOutputView;
import io.ray.streaming.state.typeinfo.serializer.TypeSerializer;
import java.io.IOException;

/**
 * Type serializer for long.
 */
public class LongSerializer extends TypeSerializer<Long> {

  public static final LongSerializer INSTANCE = new LongSerializer();

  private static final Long DEFAULT_VALUE = 0L;

  @Override
  public void serialize(Long record, DataOutputView outputView) throws IOException {
    outputView.writeLong(record);
  }

  @Override
  public Long deserialize(DataInputView inputView) throws IOException {
    return inputView.readLong();
  }

  @Override
  public TypeSerializer<Long> duplicate() {
    return this;
  }

  @Override
  public Long createInstance() {
    return DEFAULT_VALUE;
  }
}
