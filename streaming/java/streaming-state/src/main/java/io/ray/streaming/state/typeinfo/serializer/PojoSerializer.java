package io.ray.streaming.state.typeinfo.serializer;

import io.ray.streaming.state.buffer.DataInputView;
import io.ray.streaming.state.buffer.DataOutputView;
import io.ray.streaming.state.typeinfo.TypeInfoUtils;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Type serializer for Java-beans pojo.
 *
 * The serialization format for the pojo:
 * | is null(boolean) | is subclass(boolean) | field1 value(maybe is pojo) | field2 value | ...|
 */
public class PojoSerializer<T> extends TypeSerializer<T> {

  private static final byte VALUE_IS_NULL = 1;
  private static final byte NO_SUBCLASS = 2;
  private static final byte IS_SUBCLASS = 4;

  //The POJO type class
  private final Class<T> clazz;

  //The POJO type class fields.
  private final Field[] fields;
  //The POJO type class fields serializer
  private final TypeSerializer<Object>[] fieldSerializers;

  //Cache subclass serializer to avoid repeated creation
  private final Map<Class<?>, TypeSerializer<?>> subClassSerializerCache;

  //Mark the registered subclass and only need to store the index for the registered subclass.
  //The purpose can greatly save storage space.
  //private final LinkedHashMap<Class<?>, Integer> registeredClasses = new LinkedHashMap<>();
  //private final TypeSerializer<?>[] registereedSerialziers;

  private transient ClassLoader classLoader;

  private TypeSerializerConfig serializerConfig;

  public PojoSerializer(Class<T> clazz,
                        Field[] fields,
                        TypeSerializer<?>[] fieldSerializers,
                        TypeSerializerConfig serializerConfig) {

    checkNotNull(clazz);
    checkNotNull(fields);
    checkNotNull(fieldSerializers);
    this.clazz = clazz;
    this.fields = fields;
    this.fieldSerializers = (TypeSerializer<Object>[]) fieldSerializers;

    for (int i = 0; i < fields.length; i++) {
      this.fields[i].setAccessible(true);
    }

    this.subClassSerializerCache = new HashMap<>();

    this.serializerConfig = serializerConfig;

    this.classLoader = Thread.currentThread().getContextClassLoader();
  }

  @Override
  @SuppressWarnings("unchecked")
  public void serialize(T record, DataOutputView outputView) throws IOException {

    //write null tag
    if (record == null) {
      outputView.writeBoolean(true);
      return;
    } else {
      outputView.writeBoolean(false);
    }

    Class<?> valueClass = record.getClass();
    //recursive processing of subclass
    TypeSerializer subClassSerializer = null;
    boolean isSubClass = false;
    if (clazz != valueClass) {
      isSubClass = true;
      subClassSerializer = getSubClassSerializer(valueClass);
    }

    //write subclass tag
    if (isSubClass) {
      outputView.writeBoolean(true);
    } else {
      outputView.writeBoolean(false);
    }

    //write subclass name
    if (isSubClass) {
      outputView.writeUTF(valueClass.getName());
    }

    if (isSubClass) {
      subClassSerializer.serialize(record, outputView);
    } else {
      for (int i = 0; i < fields.length; i++) {
        try {
          Object fieldValue = (fields[i] != null) ? fields[i].get(record) : null;
          if (fieldValue == null) {
            outputView.writeBoolean(true);
          } else {
            outputView.writeBoolean(false);
            fieldSerializers[i].serialize(fieldValue, outputView);
          }
        } catch (IllegalAccessException e) {
          throw new RuntimeException("Serialize pojo failed.", e);
        }
      }
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public T deserialize(DataInputView inputView) throws IOException {
    T result = null;

    //read null tag
    boolean isNull = inputView.readBoolean();
    if (isNull) {
      return null;
    }

    //read subclass tag
    boolean isSubClass = inputView.readBoolean();
    Class<?> subClass;
    TypeSerializer<?> subClassSerializer;
    if (isSubClass) {
      //1. create result instance
      String subClassName = inputView.readUTF();
      try {
        subClass = Class.forName(subClassName);
      } catch (ClassNotFoundException e) {
        throw new RuntimeException("Cannot instance class", e);
      }
      subClassSerializer = getSubClassSerializer(subClass);

      result = (T) subClassSerializer.deserialize(inputView);

    } else {
      result = createInstance();

      for (int i = 0; i < fields.length; i++) {
        try {
          boolean fieldValueIsNull = inputView.readBoolean();
          if (fields[i] != null) {
            if (fieldValueIsNull) {
              fields[i].set(result, null);
            } else {
              Object field = fieldSerializers[i].deserialize(inputView);
              fields[i].set(result, field);
            }
          } else if (!fieldValueIsNull){
            fieldSerializers[i].deserialize(inputView);
          }

        } catch (IllegalAccessException e) {
          throw new RuntimeException("DeSerialize pojo failed.", e);
        }
      }
    }

    return result;
  }

  @Override
  public TypeSerializer<T> duplicate() {
    return null;
  }

  @Override
  public T createInstance() {
    if (clazz.isInterface() || Modifier.isAbstract(clazz.getModifiers())) {
      return null;
    }
    try {
      T instance = clazz.newInstance();
      initInstanceFields(instance);
      return instance;
    } catch (Exception e) {
      throw new RuntimeException("Cannot instance class.", e);
    }
  }

  //set default value
  private void initInstanceFields(T instance) {
    for (int i = 0; i < fields.length; i++) {
      try {
        fields[i].set(instance, fieldSerializers[i].createInstance());
      } catch (IllegalAccessException e) {
        throw new RuntimeException("Cannot init fields", e);
      }
    }
  }

  private TypeSerializer<?> getSubClassSerializer(Class<?> subClass) {
    TypeSerializer<?> subClassSerializer = subClassSerializerCache.get(subClass);
    if (subClassSerializer == null) {
      subClassSerializer = TypeInfoUtils.createTypeInfo(subClass).getSerializer(serializerConfig);
      subClassSerializerCache.put(subClass, subClassSerializer);
    }
    return subClassSerializer;
  }
}
