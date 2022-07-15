package io.ray.streaming.state.typeinfo.serializer;

import io.ray.streaming.state.buffer.DataInputView;
import io.ray.streaming.state.buffer.DataOutputView;
import java.io.IOException;

public class MemorySerializerViewUtil {

  public static <K> byte[] serializeKey(K key,
                                        TypeSerializer<K> keySerializer,
                                        DataOutputView keyOutputView) throws IOException {

    keyOutputView.clear();
    keySerializer.serialize(key, keyOutputView);
    return keyOutputView.getCopyOfBuffer();
  }

  public static <K> K deserializeKey(byte[] keyByteArray,
                                     TypeSerializer<K> keySerializer,
                                     DataInputView keyInputView) throws IOException {

    return deserializeKey(keyByteArray, 0, keySerializer, keyInputView);
  }

  public static <K> K deserializeKey(byte[] keyByteArray,
                                     int from,
                                     TypeSerializer<K> keySerializer,
                                     DataInputView keyInputView) throws IOException {

    keyInputView.setBuffer(keyByteArray, from, keyByteArray.length - from);
    return keySerializer.deserialize(keyInputView);
  }

  public static <V> byte[] serializeValue(V value,
                                          TypeSerializer<V> valueSerializer,
                                          DataOutputView valueOutputView) throws IOException {

    valueOutputView.clear();
    valueOutputView.writeBoolean(value == null);
    valueSerializer.serialize(value, valueOutputView);
    return valueOutputView.getCopyOfBuffer();
  }

  public static  <V> V deserializeValue(byte[] valueByteArray,
                                TypeSerializer<V> valueSerializer,
                                DataInputView valueInputView) throws IOException {
    valueInputView.setBuffer(valueByteArray);
    boolean isNUll = valueInputView.readBoolean();

    return isNUll ? null : valueSerializer.deserialize(valueInputView);
  }
}
