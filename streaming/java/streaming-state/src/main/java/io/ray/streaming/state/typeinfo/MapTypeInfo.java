package io.ray.streaming.state.typeinfo;

import io.ray.streaming.state.typeinfo.serializer.MapSerializer;
import io.ray.streaming.state.typeinfo.serializer.TypeSerializer;
import io.ray.streaming.state.typeinfo.serializer.TypeSerializerConfig;
import java.util.Map;

/** Type information for {@link Map}. */
public class MapTypeInfo<K, V> extends TypeInformation<Map<K, V>> {

  private final TypeInformation<K> keyTypeInfo;
  private final TypeInformation<V> valueTypeInfo;

  public MapTypeInfo(Class<K> keyClass, Class<V> valueClass) {
    this.keyTypeInfo = TypeInfoUtils.createTypeInfo(keyClass);
    this.valueTypeInfo = TypeInfoUtils.createTypeInfo(valueClass);
  }

  @Override
  public TypeSerializer<Map<K, V>> getSerializer(TypeSerializerConfig serializerConfig) {
    TypeSerializer<K> keyTypeSerializer = keyTypeInfo.getSerializer(serializerConfig);
    TypeSerializer<V> valueTypeSerializer = valueTypeInfo.getSerializer(serializerConfig);

    return new MapSerializer<>(keyTypeSerializer, valueTypeSerializer);
  }
}
