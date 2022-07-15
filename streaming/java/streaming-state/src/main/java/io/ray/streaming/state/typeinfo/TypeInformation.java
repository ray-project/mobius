package io.ray.streaming.state.typeinfo;

import io.ray.streaming.state.typeinfo.serializer.TypeSerializer;
import io.ray.streaming.state.typeinfo.serializer.TypeSerializerConfig;
import java.io.Serializable;

public abstract class TypeInformation<T> implements Serializable {

  public abstract TypeSerializer<T> getSerializer(TypeSerializerConfig serializerConfig);
}