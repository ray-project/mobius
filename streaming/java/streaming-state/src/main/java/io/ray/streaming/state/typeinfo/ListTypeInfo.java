package io.ray.streaming.state.typeinfo;

import com.google.common.base.MoreObjects;
import io.ray.streaming.state.typeinfo.serializer.ListSerializer;
import io.ray.streaming.state.typeinfo.serializer.TypeSerializer;
import io.ray.streaming.state.typeinfo.serializer.TypeSerializerConfig;

/** Type information for {@link java.util.List}. */
public class ListTypeInfo<T> extends TypeInformation<T> {

  private final TypeInformation<T> elementTypeInfo;

  public ListTypeInfo(Class<T> elementTypeClass) {
    this.elementTypeInfo = TypeInfoUtils.createTypeInfo(elementTypeClass);
  }

  @Override
  public TypeSerializer<T> getSerializer(TypeSerializerConfig serializerConfig) {
    TypeSerializer<T> elementTypeSerializer = elementTypeInfo.getSerializer(serializerConfig);
    return new ListSerializer(elementTypeSerializer);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("elementTypeInfo", elementTypeInfo)
        .add("serializer", ListSerializer.class)
        .toString();
  }
}
