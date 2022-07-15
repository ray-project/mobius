package io.ray.streaming.state.typeinfo.serializer;

import io.ray.streaming.state.buffer.DataInputView;
import io.ray.streaming.state.buffer.DataOutputView;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Type serializer for {@link Map}.
 *
 * The serialization format for the map:
 * | map size(int) | key data(key type serializer) | value is null(boolean) | value data(value type serializer) | ...|
 */
public class MapSerializer<K, V> extends TypeSerializer<Map<K, V>> {

  private final TypeSerializer<K> keySerializer;
  private final TypeSerializer<V> valueSerializer;

  public MapSerializer(TypeSerializer<K> keySerializer, TypeSerializer<V> valueSerializer) {
    this.keySerializer = checkNotNull(keySerializer);
    this.valueSerializer = checkNotNull(valueSerializer);
  }

  @Override
  public void serialize(Map<K, V> records, DataOutputView outputView) throws IOException {
    int size = records.size();
    outputView.writeInt(size);

    for (Map.Entry<K, V> entry : records.entrySet()) {
      keySerializer.serialize(entry.getKey(), outputView);

      //whether value is null
      if (entry.getValue() == null) {
        outputView.writeBoolean(true);
      } else {
        outputView.writeBoolean(false);
        valueSerializer.serialize(entry.getValue(), outputView);
      }
    }
  }

  @Override
  public Map<K, V> deserialize(DataInputView inputView) throws IOException {
    final int size = inputView.readInt();

    final Map<K, V> map = new HashMap<>(size);
    for (int i = 0; i < size; i++) {
      K key = keySerializer.deserialize(inputView);

      boolean isNull = inputView.readBoolean();
      V value = isNull ? null : valueSerializer.deserialize(inputView);

      map.put(key, value);
    }
    return map;
  }

  @Override
  public TypeSerializer<Map<K, V>> duplicate() {
    TypeSerializer<K> duplicateKeySerializer = keySerializer.duplicate();
    TypeSerializer<V> duplicateValueSerializer = valueSerializer.duplicate();

    if (duplicateKeySerializer == keySerializer && duplicateValueSerializer == valueSerializer) {
      return this;
    } else {
      return new MapSerializer<>(duplicateKeySerializer, duplicateValueSerializer);
    }
  }

  public TypeSerializer<K> getKeySerializer() {
    return keySerializer;
  }

  public TypeSerializer<V> getValueSerializer() {
    return valueSerializer;
  }

  @Override
  public Map<K, V> createInstance() {
    return new HashMap<>();
  }
}