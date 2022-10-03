package io.ray.streaming.common.serializer.proxy;

/** use for loading different serializer */
public abstract class SerializerProxy {

  public abstract byte[] encode(Object obj);

  public abstract Object decode(byte[] obj);
}
