package io.ray.streaming.common.serializer.proxy;

import io.ray.streaming.common.serializer.KryoUtils;

public class KryoSerializer extends SerializerProxy {

  public KryoSerializer() {}

  @Override
  public byte[] encode(Object obj) {
    return KryoUtils.writeToByteArray(obj);
  }

  @Override
  public Object decode(byte[] bytes) {
    return KryoUtils.readFromByteArray(bytes);
  }
}