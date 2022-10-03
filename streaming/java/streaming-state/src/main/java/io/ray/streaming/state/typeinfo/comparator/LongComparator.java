package io.ray.streaming.state.typeinfo.comparator;

/**
 * Type comparator for long.
 *
 * <p>TODO implement
 */
public class LongComparator extends TypeComparator<Long> {

  @Override
  public int hash(Long record) {
    return 0;
  }

  @Override
  public int compare(Long r1, Long r2) {
    return 0;
  }
}
