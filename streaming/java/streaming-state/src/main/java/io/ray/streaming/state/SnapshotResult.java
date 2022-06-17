package io.ray.streaming.state;

import com.google.common.base.MoreObjects;

public class SnapshotResult {

  public long snapshotId;

  public boolean result;

  public SnapshotResult() {} ;

  public SnapshotResult(long snapshotId, boolean result) {
    this.snapshotId = snapshotId;
    this.result = result;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("snapshotId", snapshotId)
        .toString();
  }
}
