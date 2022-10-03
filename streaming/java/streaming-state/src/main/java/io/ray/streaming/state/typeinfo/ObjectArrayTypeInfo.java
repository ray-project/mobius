package io.ray.streaming.state.typeinfo;

import io.ray.streaming.state.typeinfo.serializer.GenericArraySerializer;
import io.ray.streaming.state.typeinfo.serializer.TypeSerializer;
import io.ray.streaming.state.typeinfo.serializer.TypeSerializerConfig;

/** Type information for Object array. */
public class ObjectArrayTypeInfo<T, C> extends TypeInformation<T> {
  private final Class<T> arrayType;
  private final TypeInformation<C> componentTypeInfo;

  public ObjectArrayTypeInfo(Class<T> arrayType, TypeInformation<C> componentTypeInfo) {
    this.arrayType = arrayType;
    this.componentTypeInfo = componentTypeInfo;
  }

  @Override
  public TypeSerializer<T> getSerializer(TypeSerializerConfig serializerConfig) {
    return new GenericArraySerializer(
        arrayType.getComponentType(), componentTypeInfo.getSerializer(serializerConfig));
  }
}
