package io.ray.streaming.state.typeinfo.serializer;

import static com.google.common.base.Preconditions.checkNotNull;

import io.ray.streaming.state.buffer.DataInputView;
import io.ray.streaming.state.buffer.DataOutputView;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Type serializer for {@link List}.
 *
 * <p>The serialization format for the list: | list size(int) | element data | element data | ...|
 */
public class ListSerializer<T> extends TypeSerializer<List<T>> {

  public TypeSerializer<T> elementTypeSerializer;

  public ListSerializer(TypeSerializer<T> elementTypeSerializer) {
    checkNotNull(elementTypeSerializer);
    this.elementTypeSerializer = elementTypeSerializer;
  }

  @Override
  public void serialize(List<T> recordList, DataOutputView outputView) throws IOException {
    final int len = recordList.size();
    outputView.writeInt(len);

    for (T record : recordList) {
      elementTypeSerializer.serialize(record, outputView);
    }
  }

  @Override
  public List<T> deserialize(DataInputView inputView) throws IOException {
    final int len = inputView.readInt();

    final List<T> list = new ArrayList<>(len + 1);
    for (int i = 0; i < len; i++) {
      list.add(elementTypeSerializer.deserialize(inputView));
    }
    return list;
  }

  @Override
  public TypeSerializer<List<T>> duplicate() {
    return null;
  }

  @Override
  public List<T> createInstance() {
    return new ArrayList<>(0);
  }
}
