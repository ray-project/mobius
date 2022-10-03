package io.ray.streaming.state.typeinfo;

import com.google.common.base.MoreObjects;
import java.lang.reflect.Field;

/** Represent a field definition for {@link PojoTypeInfo} type of objects. */
public class PojoField {

  private final Field field;
  private final TypeInformation<?> fieldTypeInfo;

  public PojoField(Field field, TypeInformation<?> fieldTypeInfo) {
    this.field = field;
    this.fieldTypeInfo = fieldTypeInfo;
  }

  public Field getField() {
    return field;
  }

  public TypeInformation<?> getFieldTypeInfo() {
    return fieldTypeInfo;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("field", field)
        .add("fieldTypeInfo", fieldTypeInfo)
        .toString();
  }
}
