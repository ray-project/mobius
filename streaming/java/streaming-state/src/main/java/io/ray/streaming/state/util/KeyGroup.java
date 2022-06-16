package io.ray.streaming.state.util;

import com.google.common.base.Preconditions;
import java.io.Serializable;

public class KeyGroup implements Serializable {

  private final int startKeyGroup;
  private final int endKeyGroup;
  private final int maxShard;

  /**
   * Defines the range [startKeyGroup, endKeyGroup].
   *
   * @param startKeyGroup start of the range (inclusive)
   * @param endKeyGroup   end of the range (inclusive)
   */
  public KeyGroup(int startKeyGroup, int endKeyGroup, int maxShard) {
    Preconditions.checkArgument(startKeyGroup >= 0);
    Preconditions.checkArgument(startKeyGroup <= endKeyGroup);
    this.startKeyGroup = startKeyGroup;
    this.endKeyGroup = endKeyGroup;
    this.maxShard = maxShard;
    Preconditions.checkArgument(getNumberOfKeyGroups() >= 0, "Potential overflow detected.");
  }

  /**
   * @return The number of key-groups in the range.
   */
  public int getNumberOfKeyGroups() {
    return 1 + endKeyGroup - startKeyGroup;
  }

  public int getStartKeyGroup() {
    return startKeyGroup;
  }

  public int getEndKeyGroup() {
    return endKeyGroup;
  }

  public int getMaxShard() {
    return maxShard;
  }

  public boolean containKeyGroup(int keyGroup) {
    return startKeyGroup <= keyGroup && keyGroup <= endKeyGroup;
  }

  /**
   * Serialize KeyGroup to String
   * @param keyGroup
   * @return
   */
  public static String serialize(KeyGroup keyGroup) {
    return String.join("_",
            String.valueOf(keyGroup.startKeyGroup),
            String.valueOf(keyGroup.endKeyGroup),
            String.valueOf(keyGroup.maxShard));
  }

  /**
   * Deserialize String to KeyGroup
   * @param serializeStr
   * @return
   */
  public static KeyGroup deserialize(String serializeStr) {
    String[] args = serializeStr.split("_");
    return new KeyGroup(Integer.parseInt(args[0]),
            Integer.parseInt(args[1]),
            Integer.parseInt(args[2]));
  }

  /**
   * Judge whether there is intersection between two KeyGroup.
   * @param keyGroup1 keyGroup1
   * @param keyGroup2 keyGroup2
   * @return i{@code true} if intersection; {@code false} if the keyGroup is null
   *          or has no intersection.
   */
  public static boolean intersection(KeyGroup keyGroup1, KeyGroup keyGroup2) {
    if (keyGroup1 == null || keyGroup2 == null) {
      return false;
    }
    // 1.end >= 2.start && 1.start <= 2.end
    return keyGroup1.endKeyGroup >= keyGroup2.startKeyGroup &&
            keyGroup1.startKeyGroup <= keyGroup2.endKeyGroup;
  }

  @Override
  public int hashCode() {
    return MathUtils.longToIntWithBitMixing(startKeyGroup + endKeyGroup);
  }

  @SuppressWarnings("EqualsHashCode")
  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof KeyGroup)) {
      return false;
    }
    KeyGroup keyGroup = (KeyGroup) obj;
    return this.startKeyGroup == keyGroup.getStartKeyGroup()
            && this.endKeyGroup == keyGroup.getEndKeyGroup()
            && this.maxShard == keyGroup.maxShard;
  }

  @Override
  public String toString() {
    return "KeyGroup{" + "startKeyGroup=" + startKeyGroup
            + ", endKeyGroup=" + endKeyGroup
            + ", maxShard=" + maxShard
        + '}';
  }
}
