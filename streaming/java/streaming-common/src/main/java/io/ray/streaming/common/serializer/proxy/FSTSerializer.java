package io.ray.streaming.common.serializer.proxy;

import org.nustaq.serialization.FSTConfiguration;

public class FSTSerializer extends SerializerProxy {

  private final FSTConfiguration serializer;

  public FSTSerializer() {
    this.serializer = FSTConfiguration.createDefaultConfiguration();
  }

  @Override
  public byte[] encode(Object obj) {
    serializer.setClassLoader(Thread.currentThread().getContextClassLoader());
    return serializer.asByteArray(obj);
  }

  @Override
  public Object decode(byte[] obj) {
    serializer.setClassLoader(Thread.currentThread().getContextClassLoader());
    return serializer.asObject(obj);
  }
}
