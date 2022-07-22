package io.ray.streaming.state.typeinfo.comparator;

/**
 * Type comparator for double.
 *
 * <p>TODO implement
 */
public class DoubleComparator extends TypeComparator<Double> {

  @Override
  public int hash(Double record) {
    return 0;
  }

  @Override
  public int compare(Double r1, Double r2) {
    return 0;
  }
}
