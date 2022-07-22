package io.ray.streaming.common.serializer;

import io.ray.streaming.common.serializer.proxy.FSTSerializer;
import io.ray.streaming.common.serializer.proxy.KryoSerializer;
import io.ray.streaming.common.serializer.proxy.SerializerProxy;

public class Serializer {
  public static String SERIALIZER_TYPE_KEY = "STREAMING_SERIALIZER";
  private static final byte OTHER_TYPE_ID = 0;
  private static final byte KRYO_TYPE_ID = 1;

  private static ThreadLocal<SerializerProxy> serializerProxy;

  static {
    createSerializer();
  }

  private static void createSerializer() {
    String serializerType =
        System.getProperty(SERIALIZER_TYPE_KEY, System.getenv(SERIALIZER_TYPE_KEY));
    if (serializerType == null) {
      serializerType = "Kryo";
    }
    setSerializerProxy(serializerType);
  }

  public static synchronized void setSerializerProxy(String serializerType) {
    switch (serializerType) {
      case "FST":
        serializerProxy = ThreadLocal.withInitial(FSTSerializer::new);
        break;
      case "Kryo":
        serializerProxy = ThreadLocal.withInitial(KryoSerializer::new);
        break;
      default:
        throw new UnsupportedOperationException("Unsupported serializer type " + serializerType);
    }
  }

  public static byte[] encode(Object obj) {
    SerializerProxy serializer = Serializer.serializerProxy.get();
    return serializer.encode(obj);
  }

  public static <T> T decode(byte[] bytes) {
    SerializerProxy serializer = Serializer.serializerProxy.get();
    return (T) serializer.decode(bytes);
  }
}
