package io.ray.streaming.state.typeinfo.comparator;

/**
 * Type comparator for byte.
 *
 * TODO implement
 */
public class ByteComparator extends TypeComparator<Byte> {

  @Override
  public int hash(Byte record) {
    return 0;
  }

  @Override
  public int compare(Byte r1, Byte r2) {
    return 0;
  }
}
