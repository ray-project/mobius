package io.ray.streaming.state.typeinfo.comparator;

/**
 * Type comparator for boolean.
 *
 * <p>TODO implement
 */
public class BooleanComparator extends TypeComparator<Boolean> {

  @Override
  public int hash(Boolean record) {
    return 0;
  }

  @Override
  public int compare(Boolean r1, Boolean r2) {
    return 0;
  }
}
