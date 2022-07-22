package io.ray.streaming.state.typeinfo.comparator;

/**
 * Type comparator for string.
 *
 * <p>TODO implement
 */
public class StringComparator extends TypeComparator<String> {

  @Override
  public int hash(String record) {
    return 0;
  }

  @Override
  public int compare(String r1, String r2) {
    return 0;
  }
}
