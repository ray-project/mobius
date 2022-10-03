package io.ray.streaming.state;

import com.google.common.base.MoreObjects;

public class RollbackSnapshotResult {

  public long snapshotId;

  public boolean result;

  public RollbackSnapshotResult(long snapshotId, boolean result) {
    this.snapshotId = snapshotId;
    this.result = result;
  }

  public long getSnapshotId() {
    return snapshotId;
  }

  public boolean isResult() {
    return result;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("snapshotId", snapshotId)
        .add("result", result)
        .toString();
  }
}
