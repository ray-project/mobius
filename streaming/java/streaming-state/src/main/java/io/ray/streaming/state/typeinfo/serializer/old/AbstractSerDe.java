package io.ray.streaming.state.typeinfo.serializer.old;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import io.ray.fury.Fury;
import io.ray.fury.util.ForwardSerializer;
import io.ray.state.util.Md5Util;
import io.ray.streaming.common.serializer.KryoUtils;
import java.io.Serializable;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractSerDe implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractSerDe.class);

  protected static final ForwardSerializer furySerializer =
      new ForwardSerializer(new ForwardSerializer.DefaultFuryProxy() {
        @Override
        protected Fury newFurySerializer(ClassLoader loader) {
          // We can register custom serializers here.
          return Fury.builder()
              .withLanguage(Fury.Language.JAVA)
              .withReferenceTracking(true)
              .withClassVersionCheck(true)
              .withClassLoader(loader)
              .build();
        }
      });

  protected static final ForwardSerializer kryoSerializer =
      new ForwardSerializer(new ForwardSerializer.SerializerProxy<Kryo>() {
        private final ThreadLocal<Output> outputLocal = ThreadLocal.withInitial(
            () -> new Output(32, Integer.MAX_VALUE));

        @Override
        protected Kryo newSerializer() {
          // We can register custom serializers here.
          return new Kryo();
        }

        @Override
        protected byte[] serialize(Kryo serializer, Object obj) {
          return KryoUtils.writeToByteArray(obj);
        }

        @Override
        protected Object deserialize(Kryo serializer, byte[] bytes) {
          return KryoUtils.readFromByteArray(bytes);
        }
      });

  protected ForwardSerializer serializer;

  public void setSerializerType(String serializerType) {
    switch (serializerType) {
      case "Fury":
        serializer = furySerializer;
        break;
      case "Kryo":
        serializer = kryoSerializer;
        break;
      default:
        throw new UnsupportedOperationException("Unsupported serializer type " + serializerType);
    }
  }

  protected String generateRowKeyPrefix(String key) {
    if (StringUtils.isNotEmpty(key)) {
      String md5 = Md5Util.md5sum(key);
      if ("".equals(md5)) {
        throw new RuntimeException("Invalid value to md5:" + key);
      }
      return StringUtils.substring(md5, 0, 4) + ":" + key;
    } else {
      LOG.warn("key is empty");
      return key;
    }
  }
}