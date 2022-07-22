package io.ray.streaming.state.typeinfo;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.MoreObjects;
import io.ray.streaming.state.typeinfo.comparator.BooleanComparator;
import io.ray.streaming.state.typeinfo.comparator.ByteComparator;
import io.ray.streaming.state.typeinfo.comparator.CharacterComparator;
import io.ray.streaming.state.typeinfo.comparator.DoubleComparator;
import io.ray.streaming.state.typeinfo.comparator.FloatComparator;
import io.ray.streaming.state.typeinfo.comparator.IntegerComparator;
import io.ray.streaming.state.typeinfo.comparator.LongComparator;
import io.ray.streaming.state.typeinfo.comparator.ShortComparator;
import io.ray.streaming.state.typeinfo.comparator.StringComparator;
import io.ray.streaming.state.typeinfo.comparator.TypeComparator;
import io.ray.streaming.state.typeinfo.serializer.TypeSerializer;
import io.ray.streaming.state.typeinfo.serializer.TypeSerializerConfig;
import io.ray.streaming.state.typeinfo.serializer.basic.BooleanSerializer;
import io.ray.streaming.state.typeinfo.serializer.basic.ByteSerializer;
import io.ray.streaming.state.typeinfo.serializer.basic.CharacterSerializer;
import io.ray.streaming.state.typeinfo.serializer.basic.DoubleSerializer;
import io.ray.streaming.state.typeinfo.serializer.basic.FloatSerializer;
import io.ray.streaming.state.typeinfo.serializer.basic.IntegerSerializer;
import io.ray.streaming.state.typeinfo.serializer.basic.LongSerializer;
import io.ray.streaming.state.typeinfo.serializer.basic.ShortSerializer;
import io.ray.streaming.state.typeinfo.serializer.basic.StringSerializer;
import java.util.HashMap;
import java.util.Map;

/** Type information for primitive types(byte,int, long, double, ...) and String. */
public class BasicTypeInfo<T> extends TypeInformation<T> {

  private static final BasicTypeInfo<Byte> BYTE_TYPE_INFO =
      new BasicTypeInfo<>(Byte.class, ByteSerializer.INSTANCE, ByteComparator.class);
  private static final BasicTypeInfo<Character> CHARACTER_TYPE_INFO =
      new BasicTypeInfo<>(Character.class, CharacterSerializer.INSTANCE, CharacterComparator.class);
  private static final BasicTypeInfo<Short> SHORT_TYPE_INFO =
      new BasicTypeInfo<>(Short.class, ShortSerializer.INSTANCE, ShortComparator.class);
  private static final BasicTypeInfo<Integer> INTEGER_TYPE_INFO =
      new BasicTypeInfo<>(Integer.class, IntegerSerializer.INSTANCE, IntegerComparator.class);
  private static final BasicTypeInfo<Long> LONG_TYPE_INFO =
      new BasicTypeInfo<>(Long.class, LongSerializer.INSTANCE, LongComparator.class);
  private static final BasicTypeInfo<Float> FLOAT_TYPE_INFO =
      new BasicTypeInfo<>(Float.class, FloatSerializer.INSTANCE, FloatComparator.class);
  private static final BasicTypeInfo<Double> DOUBLE_TYPE_INFO =
      new BasicTypeInfo<>(Double.class, DoubleSerializer.INSTANCE, DoubleComparator.class);
  private static final BasicTypeInfo<Boolean> BOOLEAN_TYPE_INFO =
      new BasicTypeInfo<>(Boolean.class, BooleanSerializer.INSTANCE, BooleanComparator.class);
  private static final BasicTypeInfo<String> STRING_TYPE_INFO =
      new BasicTypeInfo<>(String.class, StringSerializer.INSTANCE, StringComparator.class);

  private static final Map<Class<?>, BasicTypeInfo<?>> TYPES = new HashMap<>();

  static {
    TYPES.put(byte.class, BYTE_TYPE_INFO);
    TYPES.put(Byte.class, BYTE_TYPE_INFO);
    TYPES.put(char.class, CHARACTER_TYPE_INFO);
    TYPES.put(Character.class, CHARACTER_TYPE_INFO);
    TYPES.put(short.class, SHORT_TYPE_INFO);
    TYPES.put(Short.class, SHORT_TYPE_INFO);
    TYPES.put(int.class, INTEGER_TYPE_INFO);
    TYPES.put(Integer.class, INTEGER_TYPE_INFO);
    TYPES.put(long.class, LONG_TYPE_INFO);
    TYPES.put(Long.class, LONG_TYPE_INFO);
    TYPES.put(float.class, FLOAT_TYPE_INFO);
    TYPES.put(Float.class, FLOAT_TYPE_INFO);
    TYPES.put(double.class, DOUBLE_TYPE_INFO);
    TYPES.put(Double.class, DOUBLE_TYPE_INFO);
    TYPES.put(boolean.class, BOOLEAN_TYPE_INFO);
    TYPES.put(Boolean.class, BOOLEAN_TYPE_INFO);
    TYPES.put(String.class, STRING_TYPE_INFO);
  }

  private final Class<T> clazz;

  private final TypeSerializer<T> serializer;

  private final Class<? extends TypeComparator<T>> comparatorClass;

  protected BasicTypeInfo(
      Class<T> clazz,
      TypeSerializer<T> serializer,
      Class<? extends TypeComparator<T>> comparatorClass) {
    this.clazz = checkNotNull(clazz);
    this.serializer = checkNotNull(serializer);
    this.comparatorClass = comparatorClass;
  }

  @Override
  public TypeSerializer<T> getSerializer(TypeSerializerConfig serializerConfig) {
    return serializer;
  }

  public Class<T> getClazz() {
    return clazz;
  }

  public Class<? extends TypeComparator<T>> getComparatorClass() {
    return comparatorClass;
  }

  @SuppressWarnings("unchecked")
  public static <X> BasicTypeInfo<X> getTypeInfoForClass(Class<X> clazzType) {
    checkNotNull(clazzType);

    return (BasicTypeInfo<X>) TYPES.get(clazzType);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("clazz", clazz)
        .add("serializer", serializer)
        .add("comparatorClass", comparatorClass)
        .toString();
  }
}
