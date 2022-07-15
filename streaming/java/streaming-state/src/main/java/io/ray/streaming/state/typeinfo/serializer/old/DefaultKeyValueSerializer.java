package io.ray.streaming.state.typeinfo.serializer.old;

public class DefaultKeyValueSerializer<K, V> extends AbstractSerDe {

  public DefaultKeyValueSerializer() {
    setDefaultIfSerializerIsEmpty();
  }

  public byte[] serializerKey(K key, boolean generateKeyPrefix) {
    if (generateKeyPrefix) {
      String keyWithPrefix = generateRowKeyPrefix(key.toString());
      return serializer.serialize(keyWithPrefix);
    } else {
      return serializer.serialize(key);
    }
  }

  public K deserializerKey(byte[] keyBytes) {
    return serializer.deserialize(keyBytes);
  }

  public byte[] serializerValue(V value) {
    return serializer.serialize(value);
  }

  public V deserializerValue(byte[] valueBytes) {
    return serializer.deserialize(valueBytes);
  }

  private void setDefaultIfSerializerIsEmpty() {
    if (serializer == null) {
      serializer = kryoSerializer;
    }
  }
}