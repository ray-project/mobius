package io.ray.streaming.state.typeinfo.comparator;

/**
 * Type comparator for short.
 *
 * TODO implement
 */
public class ShortComparator extends TypeComparator<Short> {

  @Override
  public int hash(Short record) {
    return 0;
  }

  @Override
  public int compare(Short r1, Short r2) {
    return 0;
  }
}
