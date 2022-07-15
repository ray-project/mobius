package io.ray.streaming.state.typeinfo;

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TypeInfoUtils {
  private static final Logger LOG = LoggerFactory.getLogger(TypeInfoUtils.class);

  public static <T> TypeInformation<T> createTypeInfo(Class<T> clazz) {
    return (TypeInformation<T>) createTypeInfo((Type) clazz);
  }

  public static TypeInformation<?> createTypeInfo(Type type) {
    List<Type> typeHierarchy = new ArrayList<>();
    typeHierarchy.add(type);

    TypeInformation<?> typeInformation = createTypeInfoWithTypeHierarchy(type, typeHierarchy);
    if (typeInformation == null) {
      throw new RuntimeException("Could not extract type information.");
    }

    return typeInformation;
  }

  /**
   * Type
   *    - Row types and basic type (Class), such as: int,Integer,Enum,array(int[])
   *    - Parameterized types(ParameterizedType), such as:Collection,Map
   *    - Generic Array types(GenericArrayType), such as: T[]
   *    - Type variables(TypeVariable), such as: T in List<T>
   *    - Wildcard type(WildcardType), such as: List<T extends Number>, Set<T super Integer>.
   */
  private static <T> TypeInformation<T> createTypeInfoWithTypeHierarchy(Type type, List<Type> typeHierarchy) {

    if (type instanceof ParameterizedType) {
      return (TypeInformation<T>) createTypeInfoForClass(typeConvertToClass(type), (ParameterizedType) type, typeHierarchy);
    } else if (type instanceof GenericArrayType) {
      return createTypeInfoForGenericArrayType(type);
    } else if (type instanceof TypeVariable) {
      //TODO
    } else if(type instanceof Class) {
      return createTypeInfoForClass((Class<T>) type, null, typeHierarchy);
    }

    throw new RuntimeException("Type information could not be created.");
  }

  private static <T> TypeInformation<T> createTypeInfoForGenericArrayType(Type type) {
    GenericArrayType genericArrayType = (GenericArrayType) type;

    Type componentType = genericArrayType.getGenericComponentType();
    //array type is Class type
    Class<?> componentClass = (Class<?>) componentType;
    Class<T> classArray = (Class<T>) Array.newInstance(componentClass, 0).getClass();

    List<Type> typeHierarchy = new ArrayList<>();
    typeHierarchy.add(classArray);
    return createTypeInfoForClass(classArray, null, typeHierarchy);
  }

  private static  <T> TypeInformation<T> createTypeInfoForClass(Class<T> clazz,
                                                                ParameterizedType parameterizedType,
                                                                List<Type> typeHierarchy) {

    //Object is handled as generic type info
    if (clazz.equals(Object.class)) {
      return new GenericTypeInfo<>(clazz);
    }

    //Class is handled as generic type info
    if (clazz.equals(Class.class)) {
      return new GenericTypeInfo<>(clazz);
    }

    //Recursive type s are handled as generic type info
    if (countTypeInHierarchy(clazz, typeHierarchy) > 1) {
      return new GenericTypeInfo<>(clazz);
    }

    // check for basic type
    TypeInformation<T> basicTypeInfo = BasicTypeInfo.getTypeInfoForClass(clazz);
    if (basicTypeInfo != null) {
      return basicTypeInfo;
    }

    //check for enum
    if (Enum.class.isAssignableFrom(clazz)) {
      return new EnumTypeInfo(clazz);
    }

    if (clazz.isArray()) {
      //TODO add basic array
      TypeInformation<?> componentTypeInfo = createTypeInfoWithTypeHierarchy(clazz.getComponentType(), typeHierarchy);
      return new ObjectArrayTypeInfo<>(clazz, componentTypeInfo);
    }

    //check for pojo
    PojoTypeInfo<T> pojoTypeInfo = isPojoTypeClass(clazz, typeHierarchy, parameterizedType);
    if (pojoTypeInfo != null) {
      return pojoTypeInfo;
    }

    ListTypeInfo<T> listTypeInfo = isListTypeClass(clazz, typeHierarchy);
    if (listTypeInfo != null) {
      return listTypeInfo;
    }

    //unknown type use generic type.
    return new GenericTypeInfo<>(clazz);
  }

  //Determine whether it is recursive types.
  private static int countTypeInHierarchy(Type type, List<Type> typeHierarchy) {
    int count = 0;
    for (Type t : typeHierarchy) {
      if (t == type ||
          (isClassType(type) && t == typeConvertToClass(type)) ||
          (isClassType(t) && typeConvertToClass(t) == type)) {
        count++;
      }
    }
    return count;
  }

  //ParameterizedType and Class could convert Class type.
  private static boolean isClassType(Type type) {
    return type instanceof Class<?> || type instanceof ParameterizedType;
  }

  private static Class<?> typeConvertToClass(Type type) {
    if (type instanceof Class) {
      return (Class<?>) type;
    } else if (type instanceof ParameterizedType) {
      return (Class<?>) ((ParameterizedType) type).getRawType();
    }

    throw new IllegalArgumentException("Cannot convert type to class.");
  }


  /**
   * Validation rules:
   *  1. The class access permission is public.
   *  2. The class field is public access or has corresponding setter and getter
   *  3. The class has a default no-argument constructor, and the access permission is public.
   */
  private static <T> PojoTypeInfo<T> isPojoTypeClass(Class<T> classType,
                                                      List<Type> typeHierarchy,
                                                      ParameterizedType parameterizedType) {
    try {
      //check class access type
      if (!Modifier.isPublic(classType.getModifiers())) {
        LOG.info("Class {} is not public so it cannot be used as a POJO type.", classType.getName());
        return null;
      }

      if (parameterizedType != null) {
        getTypeHierarchy(typeHierarchy, parameterizedType, Object.class);
      } else if (typeHierarchy.size() <= 1) {
        getTypeHierarchy(typeHierarchy, classType, Object.class);
      }

      //check filed type
      List<Field> fields = getAllDeclaredFields(classType);
      List<PojoField> pojoFields = new ArrayList<>();
      if (fields.size() == 0) {
        LOG.info("Class {} no fields, so it cannot be used as a POJO type.", classType);
        return null;
      }
      for (Field field : fields) {
        Type fieldType = field.getGenericType();
        if (!isValidPojoField(field, classType)) {
          LOG.info("Class {} field {} no valid POJO fields, so it cannot be used as a POJO type.", classType, field.getName());
          return null;
        }

        try {
          //Recursive field types
          List<Type> fieldTypeHierarchy = new ArrayList<>(typeHierarchy);
          fieldTypeHierarchy.add(fieldType);
          TypeInformation<?> typeInfo = createTypeInfoWithTypeHierarchy(fieldType, fieldTypeHierarchy);
          pojoFields.add(new PojoField(field, typeInfo));
        } catch (Exception e) {
          Class<?> genericClass = Object.class;
          if (isClassType(fieldType)) {
            genericClass = typeConvertToClass(fieldType);
          }
          pojoFields.add(new PojoField(field, new GenericTypeInfo<>(genericClass)));
        }
      }

      //check has the default constructor
      Constructor<T> defaultConstructor = null;
      try {
        defaultConstructor = classType.getDeclaredConstructor();
      } catch (NoSuchMethodException e) {
        LOG.info("Class {} is missing a default constructor, so it cannot be used as a POJO type.", classType);
        return null;
      }
      if (!Modifier.isPublic(defaultConstructor.getModifiers())) {
        LOG.info("Class {} default constructor is not Public, so it cannot be used as a POJO type.", classType);
        return null;
      }

      return new PojoTypeInfo<>(classType, pojoFields);
    } catch (Exception e) {
      LOG.warn("Judge pojo type failed.", e);
      return null;
    }
  }
//
//  private static <T> RowTypeInfo<T> isRowTypeClass(Class<T> clazzType, List<Type> typeHierarchy) {
//    if (Row.class.isAssignableFrom(clazzType)) {
//      List<Field> fields = getAllDeclaredFields(ObjectRow.class);
//      List<PojoField> rowFields = new ArrayList<>(fields.size());
//
//      for (Field field : fields) {
//        Type fieldType = field.getGenericType();
//
//        try {
//          //Recursive field types
//          List<Type> fieldTypeHierarchy = new ArrayList<>(typeHierarchy);
//          fieldTypeHierarchy.add(fieldType);
//          TypeInformation<?> typeInfo = createTypeInfoWithTypeHierarchy(fieldType, fieldTypeHierarchy);
//          rowFields.add(new PojoField(field, typeInfo));
//        } catch (Exception e) {
//          Class<?> genericClass = Object.class;
//          if (isClassType(fieldType)) {
//            genericClass = typeConvertToClass(fieldType);
//          }
//          rowFields.add(new PojoField(field, new GenericTypeInfo<>(genericClass)));
//        }
//      }
//      LOG.info("Class {} is row type, so creating RowTypeInfo.", clazzType);
//      return new RowTypeInfo<>(clazzType, rowFields);
//    } else {
//      return null;
//    }
//  }

//  private static <T> RowKeyTypeInfo<T> isRowKeyTypeClass(Class<T> clazzType, List<Type> typeHierarchy) {
//    if (RowKey.class.isAssignableFrom(clazzType)) {
//      List<Field> fields = getAllDeclaredFields(clazzType);
//      List<PojoField> rowKeyFields = new ArrayList<>();
//
//      for (Field field : fields) {
//        Type fieldType = field.getGenericType();
//
//        try {
//          //Recursive field types
//          List<Type> fieldTypeHierarchy = new ArrayList<>(typeHierarchy);
//          fieldTypeHierarchy.add(fieldType);
//          TypeInformation<?> typeInfo = createTypeInfoWithTypeHierarchy(fieldType, fieldTypeHierarchy);
//          rowKeyFields.add(new PojoField(field, typeInfo));
//        } catch (Exception e) {
//          Class<?> genericClass = Object.class;
//          if (isClassType(fieldType)) {
//            genericClass = typeConvertToClass(fieldType);
//          }
//          rowKeyFields.add(new PojoField(field, new GenericTypeInfo<>(genericClass)));
//        }
//      }
//      LOG.info("Class {} is row key type, so creating RowKeyTypeInfo.", clazzType);
//      return new RowKeyTypeInfo<>(clazzType, rowKeyFields);
//    } else {
//      return null;
//    }
//  }

  private static <T> ListTypeInfo<T> isListTypeClass(Class<T> clazzType, List<Type> typeHierarchy) {
    if (List.class.isAssignableFrom(clazzType)) {

      LOG.info("Class {} is list type, so creating ListTypeInfo.", clazzType);
      return new ListTypeInfo(Object.class);
    } else {
      return null;
    }
  }

  /**
   * Traverses the type hierarchy
   * @param typeHierarchy
   * @param type
   * @param stopClass
   * @return
   */
  private static Type getTypeHierarchy(List<Type> typeHierarchy, Type type, Class<?> stopClass) {
    while (!(isClassType(type) && typeConvertToClass(type).equals(stopClass))) {
      typeHierarchy.add(type);

      type = typeConvertToClass(type).getGenericSuperclass();

      if (type == null) {
        break;
      }
    }
    return type;

  }

  private static boolean isValidPojoField(Field field, Class<?> clazz) {
    //field is public access type
    if (Modifier.isPublic(field.getModifiers())) {
      return true;
    } else {
      //check getter/setter exists
      boolean hasGetter = false;
      boolean hasSetter = false;

      String fieldName = field.getName().toLowerCase().replaceAll("_", "");
      //filed descale type
      Type fieldType = field.getGenericType();

      for (Method method : clazz.getMethods()) {
        String methodName = method.getName().toLowerCase().replace("_", "");

        //check for getter
        //check (have getFieldName or isFieldName) && (method parameter list is empty) && (return type is same as field type)
        if ((methodName.equals("get" + fieldName) || methodName.equals("is" + fieldName)) &&
            (method.getParameterTypes().length == 0) &&
            (method.getReturnType().equals(fieldType))) {

          hasGetter = true;
        }

        //check for setter
        //check (have setFieldName or isFieldName) && (method parameter list length is 1)  && (method first parameter type is same as field type) && (method return type is void type)
        if ((methodName.equals("set" + fieldName) || methodName.equals("is" + fieldName)) &&
            (method.getParameterTypes().length == 1) &&
            (method.getGenericParameterTypes()[0].equals(fieldType)) &&
            (method.getReturnType().equals(Void.TYPE))) {
          hasSetter = true;
        }
      }

      return hasGetter && hasSetter;
    }

  }

  private static List<Field> getAllDeclaredFields(Class<?> clazz) {
    List<Field> fields = new ArrayList<>();
    while (clazz != null) {
      Field[] allFields = clazz.getDeclaredFields();
      for (Field field : allFields) {
        if (Modifier.isTransient(field.getModifiers()) || Modifier.isStatic(field.getModifiers())) {
          continue; //ignore for transient or static fields
        }

        fields.add(field);
      }

      clazz = clazz.getSuperclass();
    }
    return fields;
  }
}