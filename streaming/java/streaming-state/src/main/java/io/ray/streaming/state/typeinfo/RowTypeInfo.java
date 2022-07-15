package io.ray.streaming.state.typeinfo;

import io.ray.state.typeinfo.serializer.RowSerializer;
import io.ray.state.typeinfo.serializer.TypeSerializer;
import io.ray.state.typeinfo.serializer.TypeSerializerConfig;
import java.lang.reflect.Field;
import java.util.List;

/**
 *
 */
public class RowTypeInfo<T> extends TypeInformation<T> {

  private Class<T> clazz;

  private final PojoField[] fields;


  public RowTypeInfo(Class<T> clazz, List<PojoField> fields) {
    this.clazz = clazz;
    this.fields = fields.toArray(new PojoField[fields.size()]);
  }

  @Override
  public TypeSerializer<T> getSerializer(TypeSerializerConfig serializerConfig) {
    TypeSerializer<?>[] fieldsSerializer = new TypeSerializer<?>[fields.length];
    Field[] reflectedFields = new Field[fields.length];

    for (int i = 0; i < fields.length; i++) {
      fieldsSerializer[i] = fields[i].getFieldTypeInfo().getSerializer(serializerConfig);
      reflectedFields[i] = fields[i].getField();
    }

    return new RowSerializer(clazz, reflectedFields, fieldsSerializer);
  }

}
