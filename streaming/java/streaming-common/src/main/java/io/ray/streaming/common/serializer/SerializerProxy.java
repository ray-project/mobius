package io.ray.streaming.common.serializer;

import com.esotericsoftware.kryo.Kryo;
import org.nustaq.serialization.FSTConfiguration;

/**
 * use for loading different serializer
 */
public abstract class SerializerProxy {

  public abstract byte[] encode(Object obj);

  public abstract Object decode(byte[] obj);
}

class KryoSerializer extends SerializerProxy {

  public KryoSerializer() {
  }

  @Override
  public byte[] encode(Object obj) {
    return KryoUtils.writeToByteArray(obj);
  }

  @Override
  public Object decode(byte[] bytes) {
    return KryoUtils.readFromByteArray(bytes);
  }
}

class FSTSerializer extends SerializerProxy {

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
