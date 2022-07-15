package io.ray.streaming.state.typeinfo.serializer;

import com.alipay.kepler.common.type.ObjectRow;
import com.alipay.kepler.common.type.Row;
import io.ray.state.memory.DataInputView;
import io.ray.state.memory.DataOutputView;
import java.io.IOException;
import java.lang.reflect.Field;

/**
 *
 */
public class RowSerializer<T extends Row> extends TypeSerializer<T>{

  private final Class<T> clazz;

  private final Field[] fields;

  private final TypeSerializer<Object>[] fieldSerializers;

  public RowSerializer(Class<T> clazz,
                       Field[] fields,
                       TypeSerializer<?>[] fieldSerializers) {

    this.clazz = clazz;
    this.fields = fields;
    this.fieldSerializers = (TypeSerializer<Object>[]) fieldSerializers;

    for (int i = 0; i < fields.length; i++) {
      this.fields[i].setAccessible(true);
    }
  }

  @Override
  public void serialize(T record, DataOutputView outputView) throws IOException {
    if (record == null) {
      outputView.writeBoolean(true);
    } else {
      outputView.writeBoolean(false);
    }

    int objectRowFieldsLength = record.getFields(null).length;
    outputView.writeInt(objectRowFieldsLength);
    for (int i = 0; i < fields.length; i++) {
      try {
        Object fieldValue = (fields[i] != null) ? fields[i].get(record) : null;
        if (fieldValue == null) {
          outputView.writeBoolean(true);
        } else {
          outputView.writeBoolean(false);
          fieldSerializers[i].serialize(fieldValue, outputView);
        }
      } catch (IllegalAccessException e) {
        throw new RuntimeException("Serialize Row class failed.", e);
      }
    }
  }

  @Override
  public T deserialize(DataInputView inputView) throws IOException {
    //read null tag
    boolean isNull = inputView.readBoolean();
    if (isNull) {
      return null;
    }

    int objectRowFieldLength = inputView.readInt();
    ObjectRow result = (ObjectRow) ObjectRow.create(objectRowFieldLength);
    initInstanceFields(result);

    for (int i = 0; i < fields.length; i++) {
      try {
        boolean fieldValueIsNull = inputView.readBoolean();
        if (fields[i] != null) {
          if (fieldValueIsNull) {
            fields[i].set(result, null);
          } else {
            Object field = fieldSerializers[i].deserialize(inputView);
            fields[i].set(result, field);
          }
        } else if (!fieldValueIsNull){
          fieldSerializers[i].deserialize(inputView);
        }
      } catch (IllegalAccessException e) {
        throw new RuntimeException("DeSerialize pojo failed.", e);
      }
    }

    return (T) result;
  }
  @Override
  public TypeSerializer<T> duplicate() {
    return null;
  }

  @Override
  public T createInstance() {
    return null;
  }

  //set default value
  private void initInstanceFields(ObjectRow instance) {
    for (int i = 0; i < fields.length; i++) {
      try {
        fields[i].set(instance, fieldSerializers[i].createInstance());
      } catch (IllegalAccessException e) {
        throw new RuntimeException("Cannot init fields", e);
      }
    }
  }
}
