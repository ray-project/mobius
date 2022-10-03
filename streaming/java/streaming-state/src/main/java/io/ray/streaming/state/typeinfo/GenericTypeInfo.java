package io.ray.streaming.state.typeinfo;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.MoreObjects;
import io.ray.streaming.state.typeinfo.serializer.TypeSerializer;
import io.ray.streaming.state.typeinfo.serializer.TypeSerializerConfig;
import io.ray.streaming.state.typeinfo.serializer.kryo.KryoSerializer;

/** RayState does no recognize the data type, the serializer use kryo. */
public class GenericTypeInfo<T> extends TypeInformation<T> {

  private final Class<T> typeClass;

  public GenericTypeInfo(Class<T> typeClass) {
    checkNotNull(typeClass);
    this.typeClass = typeClass;
  }

  @Override
  public TypeSerializer<T> getSerializer(TypeSerializerConfig serializerConfig) {
    if (serializerConfig != null) {
      return new KryoSerializer<>(typeClass, serializerConfig.getKryoRegistrations());
    } else {
      return new KryoSerializer<>(typeClass);
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("typeClass", typeClass)
        .add("serializer", KryoSerializer.class)
        .toString();
  }
}
