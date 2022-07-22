package io.ray.streaming.state.typeinfo;

import static com.google.common.base.Preconditions.checkNotNull;

import io.ray.streaming.state.typeinfo.serializer.PojoSerializer;
import io.ray.streaming.state.typeinfo.serializer.TypeSerializer;
import io.ray.streaming.state.typeinfo.serializer.TypeSerializerConfig;
import java.lang.reflect.Field;
import java.util.List;

/**
 * Type information for Java Beans style types.
 *
 * <p>Note: Pojo define: 1. It is a public class, and not a non-static inner class. 2. It has a
 * public no-argument constructor. 3. All fields are either public or have public getters and
 * setters.
 */
public class PojoTypeInfo<T> extends TypeInformation<T> {

  private Class<T> clazz;

  private final PojoField[] fields;

  public PojoTypeInfo(Class<T> typeClass, List<PojoField> fields) {
    checkNotNull(typeClass);

    this.clazz = typeClass;
    this.fields = fields.toArray(new PojoField[fields.size()]);
  }

  @Override
  public TypeSerializer<T> getSerializer(TypeSerializerConfig serializerConfig) {
    TypeSerializer<?>[] fieldsSerializer = new TypeSerializer<?>[fields.length];
    Field[] reflectiveFields = new Field[fields.length];

    for (int i = 0; i < fields.length; i++) {
      fieldsSerializer[i] = fields[i].getFieldTypeInfo().getSerializer(serializerConfig);
      reflectiveFields[i] = fields[i].getField();
    }

    return new PojoSerializer<>(clazz, reflectiveFields, fieldsSerializer, serializerConfig);
  }
}
