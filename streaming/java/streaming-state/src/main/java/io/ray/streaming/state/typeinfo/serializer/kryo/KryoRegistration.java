package io.ray.streaming.state.typeinfo.serializer.kryo;

import static com.google.common.base.Preconditions.checkNotNull;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.factories.ReflectionSerializerFactory;
import java.io.Serializable;

public class KryoRegistration implements Serializable {

  private final Class<?> registeredClass;

  private final Class<? extends Serializer> serializerClass;

  public KryoRegistration(Class<?> registeredClass) {
    checkNotNull(registeredClass);

    this.registeredClass = registeredClass;
    this.serializerClass = null;
  }

  public KryoRegistration(
      Class<?> registeredClass, Class<? extends Serializer<?>> serializerClass) {
    checkNotNull(registeredClass);
    checkNotNull(serializerClass);

    this.registeredClass = registeredClass;
    this.serializerClass = serializerClass;
  }

  public Class<?> getRegisteredClass() {
    return registeredClass;
  }

  public Serializer<?> getSerializer(Kryo kryo) {
    if (serializerClass == null) {
      return null;
    }
    return ReflectionSerializerFactory.makeSerializer(kryo, serializerClass, registeredClass);
  }
}
