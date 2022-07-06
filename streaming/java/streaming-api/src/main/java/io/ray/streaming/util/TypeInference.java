package io.ray.streaming.util;

import com.google.common.base.CaseFormat;
import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;
import io.ray.streaming.common.tuple.Tuple2;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * NOTICE: This is a tmp impl that is copied from io.ant.fury,
 * for the sake of open source. Will be replaced one day.
 */
public class TypeInference {

  /* =============== Copied from io.ray.fury.codegen.TypeUtils */
  private static final TypeToken<?> PRIMITIVE_BYTE_TYPE = TypeToken.of(byte.class);
  private static final TypeToken<?> PRIMITIVE_BOOLEAN_TYPE = TypeToken.of(boolean.class);
  private static final TypeToken<?> PRIMITIVE_CHAR_TYPE = TypeToken.of(char.class);
  private static final TypeToken<?> PRIMITIVE_SHORT_TYPE = TypeToken.of(short.class);
  private static final TypeToken<?> PRIMITIVE_INT_TYPE = TypeToken.of(int.class);
  private static final TypeToken<?> PRIMITIVE_LONG_TYPE = TypeToken.of(long.class);
  private static final TypeToken<?> PRIMITIVE_FLOAT_TYPE = TypeToken.of(float.class);
  private static final TypeToken<?> PRIMITIVE_DOUBLE_TYPE = TypeToken.of(double.class);

  private static final TypeToken<?> BYTE_TYPE = TypeToken.of(Byte.class);
  private static final TypeToken<?> BOOLEAN_TYPE = TypeToken.of(Boolean.class);
  private static final TypeToken<?> CHAR_TYPE = TypeToken.of(Character.class);
  private static final TypeToken<?> SHORT_TYPE = TypeToken.of(Short.class);
  private static final TypeToken<?> INT_TYPE = TypeToken.of(Integer.class);
  private static final TypeToken<?> LONG_TYPE = TypeToken.of(Long.class);
  private static final TypeToken<?> FLOAT_TYPE = TypeToken.of(Float.class);
  private static final TypeToken<?> DOUBLE_TYPE = TypeToken.of(Double.class);

  private static final TypeToken<?> STRING_TYPE = TypeToken.of(String.class);
  private static final TypeToken<?> BIG_DECIMAL_TYPE = TypeToken.of(BigDecimal.class);
  private static final TypeToken<?> BIG_INTEGER_TYPE = TypeToken.of(BigInteger.class);
  private static final TypeToken<?> DATE_TYPE = TypeToken.of(Date.class);
  private static final TypeToken<?> LOCAL_DATE_TYPE = TypeToken.of(LocalDate.class);
  private static final TypeToken<?> TIMESTAMP_TYPE = TypeToken.of(Timestamp.class);
  private static final TypeToken<?> INSTANT_TYPE = TypeToken.of(Instant.class);
  private static final TypeToken<?> BINARY_TYPE = TypeToken.of(byte[].class);
  /* =============== End copied from io.ray.fury.codegen.TypeUtils End */

  /**
   * bean fields should all be in SUPPORTED_TYPES, enum, array/ITERABLE_TYPE/MAP_TYPE type, bean type.
   * <p>If bean fields is ITERABLE_TYPE/MAP_TYPE, the type should be super class(inclusive) of List/Set/Map,
   * or else should be a no arg constructor.</p>
   */
  private static Set<TypeToken<?>> SUPPORTED_TYPES = new HashSet<>();
  static {
    SUPPORTED_TYPES.add(PRIMITIVE_BYTE_TYPE);
    SUPPORTED_TYPES.add(PRIMITIVE_BOOLEAN_TYPE);
    SUPPORTED_TYPES.add(PRIMITIVE_CHAR_TYPE);
    SUPPORTED_TYPES.add(PRIMITIVE_SHORT_TYPE);
    SUPPORTED_TYPES.add(PRIMITIVE_INT_TYPE);
    SUPPORTED_TYPES.add(PRIMITIVE_LONG_TYPE);
    SUPPORTED_TYPES.add(PRIMITIVE_FLOAT_TYPE);
    SUPPORTED_TYPES.add(PRIMITIVE_DOUBLE_TYPE);

    SUPPORTED_TYPES.add(BYTE_TYPE);
    SUPPORTED_TYPES.add(BOOLEAN_TYPE);
    SUPPORTED_TYPES.add(CHAR_TYPE);
    SUPPORTED_TYPES.add(SHORT_TYPE);
    SUPPORTED_TYPES.add(INT_TYPE);
    SUPPORTED_TYPES.add(LONG_TYPE);
    SUPPORTED_TYPES.add(FLOAT_TYPE);
    SUPPORTED_TYPES.add(DOUBLE_TYPE);

    SUPPORTED_TYPES.add(STRING_TYPE);
    SUPPORTED_TYPES.add(BIG_DECIMAL_TYPE);
    // SUPPORTED_TYPES.add(BIG_INTEGER_TYPE);
    SUPPORTED_TYPES.add(DATE_TYPE);
    SUPPORTED_TYPES.add(LOCAL_DATE_TYPE);
    SUPPORTED_TYPES.add(TIMESTAMP_TYPE);
    SUPPORTED_TYPES.add(INSTANT_TYPE);
  }

  private static Type ITERATOR_RETURN_TYPE;
  private static Type NEXT_RETURN_TYPE;
  private static Type KEY_SET_RETURN_TYPE;
  private static Type VALUES_RETURN_TYPE;

  /* Field Constants */
  private static final int MAX_PRECISION = 38;
  private static final int MAX_SCALE = 18;
  // Array item field default name
  private static final String ARRAY_ITEM_NAME = "item";

  private static final TypeToken<?> ITERABLE_TYPE = TypeToken.of(Iterable.class);
  private static final TypeToken<?> COLLECTION_TYPE = TypeToken.of(Collection.class);
  private static final TypeToken<?> LIST_TYPE = TypeToken.of(List.class);
  private static final TypeToken<?> SET_TYPE = TypeToken.of(Set.class);
  private static final TypeToken<?> MAP_TYPE = TypeToken.of(Map.class);

  static {
    try {
      ITERATOR_RETURN_TYPE = Iterable.class.getMethod("iterator").getGenericReturnType();
      NEXT_RETURN_TYPE = Iterator.class.getMethod("next").getGenericReturnType();
      KEY_SET_RETURN_TYPE = Map.class.getMethod("keySet").getGenericReturnType();
      VALUES_RETURN_TYPE = Map.class.getMethod("values").getGenericReturnType();
    } catch (NoSuchMethodException e) {
      throw new Error(e); // should be impossible
    }
  }



  /**
   * @return element type of a iterable
   */
  public static TypeToken<?> getElementType(TypeToken<?> typeToken) {
    @SuppressWarnings("unchecked")
    TypeToken<?> supertype = ((TypeToken<? extends Iterable<?>>) typeToken).getSupertype(Iterable.class);
    return supertype.resolveType(ITERATOR_RETURN_TYPE).resolveType(NEXT_RETURN_TYPE);
  }

  public static Schema inferSchema(java.lang.reflect.Type type) {
    return inferSchema(TypeToken.of(type));
  }

  /**
   * @param typeToken bean class type
   * @return schema of a class
   */
  public static Schema inferSchema(TypeToken<?> typeToken) {
    Field field = inferField(typeToken);
    Preconditions.checkArgument(
        field.getType().getTypeID() == ArrowType.ArrowTypeID.Struct);
    return new Schema(field.getChildren());
  }

  private static Field inferField(TypeToken<?> typeToken) {
    return inferField("", typeToken, new LinkedHashSet<>());
  }

  /**
   * When type is both iterable and bean, we take it as iterable in row-format.
   * Note circular references in bean class is not allowed.
   *
   * @return DataType of a typeToken
   */
  private static org.apache.arrow.vector.types.pojo.Field inferField(
      String name, TypeToken<?> typeToken, LinkedHashSet<Class<?>> seenTypeSet) {
    Class<?> rawType = typeToken.getRawType();
    if (rawType == boolean.class) {
      return field(name, notNullFieldType(ArrowType.Bool.INSTANCE));
    } else if (rawType == byte.class) {
      return field(name, notNullFieldType(new ArrowType.Int(8, true)));
    } else if (rawType == short.class) {
      return field(name, notNullFieldType(new ArrowType.Int(16, true)));
    } else if (rawType == int.class) {
      return field(name, notNullFieldType(new ArrowType.Int(32, true)));
    } else if (rawType == long.class) {
      return field(name, notNullFieldType(new ArrowType.Int(64, true)));
    } else if (rawType == float.class) {
      return field(name, notNullFieldType(new ArrowType.FloatingPoint(
          FloatingPointPrecision.SINGLE)));
    } else if (rawType == double.class) {
      return field(name, notNullFieldType(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)));
    } else if (rawType == Boolean.class) {
      return field(name, FieldType.nullable((ArrowType.Bool.INSTANCE)));
    } else if (rawType == Byte.class) {
      return field(name, FieldType.nullable((new ArrowType.Int(8, true))));
    } else if (rawType == Short.class) {
      return field(name, FieldType.nullable((new ArrowType.Int(16, true))));
    } else if (rawType == Integer.class) {
      return field(name, FieldType.nullable((new ArrowType.Int(32, true))));
    } else if (rawType == Long.class) {
      return field(name, FieldType.nullable((new ArrowType.Int(64, true))));
    } else if (rawType == Float.class) {
      return field(name, FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)));
    } else if (rawType == Double.class) {
      return field(name, FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)));
    } else if (rawType == java.math.BigDecimal.class) {
      return field(name, FieldType.nullable(new ArrowType.Decimal(
          MAX_PRECISION, MAX_SCALE)));
    } else if (rawType == java.math.BigInteger.class) {
      return field(name, FieldType.nullable(new ArrowType.Decimal(
          MAX_PRECISION, 0)));
    } else if (rawType == java.time.LocalDate.class) {
      return field(name, FieldType.nullable(new ArrowType.Date(DateUnit.DAY)));
    } else if (rawType == java.sql.Date.class) {
      return field(name, FieldType.nullable(new ArrowType.Date(DateUnit.DAY)));
    } else if (rawType == java.sql.Timestamp.class) {
      return field(name, FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)));
    } else if (rawType == java.time.Instant.class) {
      return field(name, FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)));
    } else if (rawType == String.class) {
      return field(name, FieldType.nullable(ArrowType.Utf8.INSTANCE));
    } else if (rawType.isEnum()) {
      return field(name, FieldType.nullable(ArrowType.Utf8.INSTANCE));
    } else if (rawType.isArray()) { // array
      Field f = inferField(ARRAY_ITEM_NAME,
          Objects.requireNonNull(typeToken.getComponentType()), seenTypeSet);
      return arrayField(name, f);
    } else if (ITERABLE_TYPE.isSupertypeOf(typeToken)) { // iterable
      // when type is both iterable and bean, we take it as iterable in row-format
      Field f = inferField(ARRAY_ITEM_NAME, getElementType(typeToken), seenTypeSet);
      return arrayField(name, f);
    } else if (MAP_TYPE.isSupertypeOf(typeToken)) {
      Tuple2<TypeToken<?>, TypeToken<?>> kvType = getMapKeyValueType(typeToken);
      Field keyField = inferField(MapVector.KEY_NAME, kvType.f0, seenTypeSet);
      // Map's keys must be non-nullable
      FieldType keyFieldType = new FieldType(
          false, keyField.getType(), keyField.getDictionary(), keyField.getMetadata());
      keyField = new Field(keyField.getName(), keyFieldType, keyField.getChildren());
      Field valueField = inferField(MapVector.VALUE_NAME, kvType.f1, seenTypeSet);
      return mapField(name, keyField, valueField);
    } else if (isBean(rawType)) { // bean field
      if (seenTypeSet.contains(rawType)) {
        String msg = String.format("circular references in bean class is not allowed, but got " +
            "%s in %s", rawType, seenTypeSet);
        throw new UnsupportedOperationException(msg);
      }
      List<Field> fields = Descriptor.getDescriptors(rawType)
          .stream()
          .map(descriptor -> {
            LinkedHashSet<Class<?>> newSeenTypeSet = new LinkedHashSet<>(seenTypeSet);
            newSeenTypeSet.add(rawType);
            String n = CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, descriptor.getName());
            return inferField(n, descriptor.getTypeToken(), newSeenTypeSet);
          })
          .collect(Collectors.toList());
      return structField(name, true, fields);
    } else {
      throw new UnsupportedOperationException(
          String.format("Unsupported type %s for field %s, seen type set is %s",
              typeToken, name, seenTypeSet));
    }
  }

  /**
   * @return key/value type of a map
   */
  private static Tuple2<TypeToken<?>, TypeToken<?>> getMapKeyValueType(TypeToken<?> typeToken) {
    @SuppressWarnings("unchecked")
    TypeToken<?> supertype = ((TypeToken<? extends Map<?, ?>>) typeToken).getSupertype(Map.class);
    TypeToken<?> keyType = getElementType(supertype.resolveType(KEY_SET_RETURN_TYPE));
    TypeToken<?> valueType = getElementType(supertype.resolveType(VALUES_RETURN_TYPE));
    return Tuple2.of(keyType, valueType);
  }

  /* ============= Belows are all copied from io.ray.fury.types.DataTypes  =============== */

  /**
   * @see #isBean(com.google.common.reflect.TypeToken)
   */
  public static boolean isBean(Class<?> clz) {
    return isBean(TypeToken.of(clz));
  }

  /**
   * @see #isBean(com.google.common.reflect.TypeToken)
   */
  public static boolean isBean(java.lang.reflect.Type type) {
    return isBean(TypeToken.of(type));
  }

  public static boolean isBean(TypeToken<?> typeToken) {
    return isBean(typeToken, new LinkedHashSet<>());
  }

  private static boolean isBean(TypeToken<?> typeToken, LinkedHashSet<TypeToken> walkedTypePath) {
    Class<?> cls = typeToken.getRawType();
    if (Modifier.isAbstract(cls.getModifiers()) || Modifier.isInterface(cls.getModifiers())) {
      return false;
    }
    // since we need to access class in generated code in our package, the class must be public
    // if ReflectionUtils.hasNoArgConstructor(cls) return false, we use Unsafe to create object.
    if (Modifier.isPublic(cls.getModifiers())) {
      // bean class can be static nested class, but can't be not a non-static inner class
      if (cls.getEnclosingClass() != null && !Modifier.isStatic(cls.getModifiers())) {
        return false;
      }
      LinkedHashSet<TypeToken> newTypePath = new LinkedHashSet<>(walkedTypePath);
      newTypePath.add(typeToken);
      if (cls == Object.class) {
        // return false for typeToken that point to un-specialized generic type.
        return false;
      }

      boolean maybe = !SUPPORTED_TYPES.contains(typeToken)
          && !typeToken.isArray()
          && !cls.isEnum()
          && !ITERABLE_TYPE.isSupertypeOf(typeToken)
          && !MAP_TYPE.isSupertypeOf(typeToken);
      if (maybe) {
        return Descriptor.getDescriptors(cls).stream()
            .allMatch(d -> {
              TypeToken<?> t = d.getTypeToken();
              // do field modifiers and getter/setter validation here, not in getDescriptors.
              // If Modifier.isFinal(d.getModifiers()), use reflection
              // private field that doesn't have getter/setter will be handled by reflection.
              return isSupported(t, newTypePath) || isBean(t, newTypePath);
            });
      } else {
        return false;
      }
    } else {
      return false;
    }
  }

  /**
   * Check if <code>typeToken</code> is supported by row-format
   */
  private static boolean isSupported(TypeToken<?> typeToken) {
    return isSupported(typeToken, new LinkedHashSet<>());
  }

  private static boolean isSupported(TypeToken<?> typeToken, LinkedHashSet<TypeToken> walkedTypePath) {
    Class<?> cls = typeToken.getRawType();
    if (!Modifier.isPublic(cls.getModifiers())) {
      return false;
    }
    if (cls == Object.class) {
      // return true for typeToken that point to un-specialized generic type, take it as a black box.
      return true;
    }
    if (SUPPORTED_TYPES.contains(typeToken)) {
      return true;
    } else if (typeToken.isArray()) {
      return isSupported(Objects.requireNonNull(typeToken.getComponentType()));
    } else if (ITERABLE_TYPE.isSupertypeOf(typeToken)) {
      boolean isSuperOfArrayList = cls.isAssignableFrom(ArrayList.class);
      boolean isSuperOfHashSet = cls.isAssignableFrom(HashSet.class);
      if ((!isSuperOfArrayList && !isSuperOfHashSet)
          && (cls.isInterface() || Modifier.isAbstract(cls.getModifiers()))) {
        return false;
      }
      return isSupported(getElementType(typeToken));
    } else if (MAP_TYPE.isSupertypeOf(typeToken)) {
      boolean isSuperOfHashMap = cls.isAssignableFrom(HashMap.class);
      if (!isSuperOfHashMap && (cls.isInterface() || Modifier.isAbstract(cls.getModifiers()))) {
        return false;
      }
      Tuple2<TypeToken<?>, TypeToken<?>> mapKeyValueType = getMapKeyValueType(typeToken);
      return isSupported(mapKeyValueType.f0) && isSupported(mapKeyValueType.f1);
    } else {
      if (walkedTypePath.contains(typeToken)) {
        throw new UnsupportedOperationException("cyclic type is not supported. walkedTypePath: " + walkedTypePath);
      } else {
        LinkedHashSet<TypeToken> newTypePath = new LinkedHashSet<>(walkedTypePath);
        newTypePath.add(typeToken);
        return isBean(typeToken, newTypePath);
      }
    }
  }


  /* ========================= field utils ========================= */
  private static Field field(String name, FieldType fieldType) {
    return new Field(name, fieldType, null);
  }

  public static Field field(
      String name, boolean nullable, ArrowType type, Field... children) {
    return new Field(name, new FieldType(nullable, type, null), Arrays.asList(children));
  }

  public static Field field(
      String name, boolean nullable, ArrowType type, List<Field> children) {
    return new Field(name, new FieldType(nullable, type, null), children);
  }

  private static Field arrayField(String name, Field valueField) {
    return new Field(name, FieldType.nullable(ArrowType.List.INSTANCE),
        Collections.singletonList(valueField));
  }

  public static Field structField(boolean nullable, Field... fields) {
    return structField("", nullable, fields);
  }

  public static Field structField(String name, boolean nullable, Field... fields) {
    return field(name, nullable, ArrowType.Struct.INSTANCE, fields);
  }

  public static Field structField(String name, boolean nullable, List<Field> fields) {
    return field(name, nullable, ArrowType.Struct.INSTANCE, fields);
  }

  /**
   * Map data is nested data where each value is a variable number of
   * key-item pairs.  Maps can be recursively nested, for example
   * map(utf8, map(utf8, int32)). see more about MapType in type.h
   */
  public static Field mapField(String name, Field keyField, Field itemField) {
    Preconditions.checkArgument(!keyField.isNullable(),
        "Map's keys must be non-nullable");
    // Map's key-item pairs must be non-nullable structs
    Field valueField = structField(false, keyField, itemField);
    return field(name, true, new ArrowType.Map(false), valueField);
  }

  private static FieldType notNullFieldType(ArrowType type) {
    return new FieldType(false, type, null);
  }

}
