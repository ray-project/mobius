package io.ray.streaming.state.typeinfo;

import static com.google.common.base.Preconditions.checkNotNull;

import io.ray.streaming.state.typeinfo.serializer.EnumSerializer;
import io.ray.streaming.state.typeinfo.serializer.TypeSerializer;
import io.ray.streaming.state.typeinfo.serializer.TypeSerializerConfig;

/** Type information for {@link Enum}. */
public class EnumTypeInfo<T extends Enum> extends TypeInformation<T> {

  private final Class<T> typeClass;

  public EnumTypeInfo(Class<T> typeClass) {
    checkNotNull(typeClass);
    this.typeClass = typeClass;
  }

  @SuppressWarnings("unchecked")
  @Override
  public TypeSerializer<T> getSerializer(TypeSerializerConfig serializerConfig) {
    return new EnumSerializer<>(typeClass);
  }
}
