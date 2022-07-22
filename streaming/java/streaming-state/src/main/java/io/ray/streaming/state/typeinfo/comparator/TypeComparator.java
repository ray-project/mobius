package io.ray.streaming.state.typeinfo.comparator;

import java.io.Serializable;

/** Type comparator abstract base class. */
public abstract class TypeComparator<T> implements Serializable {

  /** Computes a hash value for the give record. */
  public abstract int hash(T record);

  /**
   * Compares two records in object. The return value indicates the order of the tow in the same way
   * as defined by {@link java.util.Comparator#compare(Object, Object)}.
   */
  public abstract int compare(T r1, T r2);
}
