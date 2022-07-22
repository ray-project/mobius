package io.ray.streaming.state.typeinfo.serializer.basic;

import io.ray.streaming.state.buffer.DataInputView;
import io.ray.streaming.state.buffer.DataOutputView;
import io.ray.streaming.state.typeinfo.serializer.TypeSerializer;
import java.io.IOException;

/** Type serializer for char. */
public class CharacterSerializer extends TypeSerializer<Character> {

  public static final CharacterSerializer INSTANCE = new CharacterSerializer();

  private static final Character DEFAULT_VALUE = (char) 0;

  @Override
  public void serialize(Character record, DataOutputView outputView) throws IOException {
    outputView.writeChar(record);
  }

  @Override
  public Character deserialize(DataInputView inputView) throws IOException {
    return inputView.readChar();
  }

  @Override
  public TypeSerializer<Character> duplicate() {
    return this;
  }

  @Override
  public Character createInstance() {
    return DEFAULT_VALUE;
  }
}
