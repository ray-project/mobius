package io.ray.streaming.common.serializer.proxy;

import io.ray.streaming.common.serializer.KryoUtils;
import org.nustaq.serialization.FSTConfiguration;

/** use for loading different serializer */
public abstract class SerializerProxy {

  public abstract byte[] encode(Object obj);

  public abstract Object decode(byte[] obj);
}

