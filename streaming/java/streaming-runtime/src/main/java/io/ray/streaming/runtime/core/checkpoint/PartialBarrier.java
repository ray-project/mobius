package io.ray.streaming.runtime.core.checkpoint;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import java.io.Serializable;

public class PartialBarrier implements Serializable {

  private long globalCheckpointId;
  private long partialCheckpointId;

  public PartialBarrier(long globalCheckpointId, long partialCheckpointId) {
    this.globalCheckpointId = globalCheckpointId;
    this.partialCheckpointId = partialCheckpointId;
  }

  public long getGlobalCheckpointId() {
    return globalCheckpointId;
  }

  public long getPartialCheckpointId() {
    return partialCheckpointId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PartialBarrier that = (PartialBarrier) o;
    return globalCheckpointId == that.globalCheckpointId
        && partialCheckpointId == that.partialCheckpointId;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(globalCheckpointId, partialCheckpointId);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("globalCkptId", globalCheckpointId)
        .add("partialCkptId", partialCheckpointId)
        .toString();
  }
}
