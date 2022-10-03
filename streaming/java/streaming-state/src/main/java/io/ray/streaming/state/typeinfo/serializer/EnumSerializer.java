package io.ray.streaming.state.typeinfo.serializer;

import static com.google.common.base.Preconditions.checkNotNull;

import io.ray.streaming.state.buffer.DataInputView;
import io.ray.streaming.state.buffer.DataOutputView;
import java.io.IOException;
import java.util.EnumMap;
import java.util.Map;

/**
 * Type serializer for {@link Enum}.
 *
 * <p>The serialization format for the Enum: | enum ordinal|
 */
public class EnumSerializer<T extends Enum<T>> extends TypeSerializer<T> {

  private final Class<T> enumClass;

  /** Enum element to ordinal map */
  private Map<T, Integer> valueToOrdinal;
  /** Enum values */
  private T[] values;

  public EnumSerializer(Class<T> enumClass) {
    checkNotNull(enumClass);

    this.enumClass = enumClass;

    this.values = enumClass.getEnumConstants();
    this.valueToOrdinal = new EnumMap<>(enumClass);
    for (int i = 0; i < values.length; i++) {
      valueToOrdinal.put(values[i], i);
    }
  }

  @Override
  public void serialize(T record, DataOutputView outputView) throws IOException {
    outputView.writeInt(valueToOrdinal.get(record));
  }

  @Override
  public T deserialize(DataInputView inputView) throws IOException {
    return values[inputView.readInt()];
  }

  @Override
  public TypeSerializer<T> duplicate() {
    return null;
  }

  @Override
  public T createInstance() {
    return values[0];
  }
}
