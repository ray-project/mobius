package io.ray.streaming.state.typeinfo.comparator;

/**
 * Type comparator for int.
 *
 * <p>TODO implement
 */
public class IntegerComparator extends TypeComparator<Integer> {

  @Override
  public int hash(Integer record) {
    return 0;
  }

  @Override
  public int compare(Integer r1, Integer r2) {
    return 0;
  }
}
