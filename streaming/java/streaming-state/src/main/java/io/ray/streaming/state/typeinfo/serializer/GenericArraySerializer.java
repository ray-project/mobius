package io.ray.streaming.state.typeinfo.serializer;

import io.ray.state.memory.DataInputView;
import io.ray.state.memory.DataOutputView;
import java.io.IOException;
import java.lang.reflect.Array;

/**
 * Type serializer for Object[].
 */
public class GenericArraySerializer<C> extends TypeSerializer<C[]> {
  private final Class<C> componentClass;
  private final TypeSerializer<C> componentSerializer;

  public GenericArraySerializer(Class<C> componentClass,
                                TypeSerializer<C> componentSerializer) {
    this.componentClass = componentClass;
    this.componentSerializer = componentSerializer;
  }

  @Override
  public void serialize(C[] record, DataOutputView outputView) throws IOException {
    outputView.writeInt(record.length);
    for (int i = 0; i < record.length; i++) {
      C item = record[i];
      if (item == null) {
        outputView.writeBoolean(true);
      } else {
        outputView.writeBoolean(false);
        componentSerializer.serialize(item, outputView);
      }
    }
  }

  @Override
  public C[] deserialize(DataInputView inputView) throws IOException {
    int length = inputView.readInt();
    C[] arrayResult = (C[]) Array.newInstance(componentClass, length);
    for (int i = 0; i < length; i++) {
      boolean isNull = inputView.readBoolean();
      if (isNull) {
        arrayResult[i] = null;
      } else {
        arrayResult[i] = componentSerializer.deserialize(inputView);
      }
    }
    return arrayResult;
  }

  @Override
  public TypeSerializer<C[]> duplicate() {
    return null;
  }

  @Override
  public C[] createInstance() {
    return (C[]) Array.newInstance(componentClass, 0);
  }
}