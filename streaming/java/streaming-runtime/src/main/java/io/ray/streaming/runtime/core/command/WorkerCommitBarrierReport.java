package io.ray.streaming.runtime.core.command;

import com.google.common.base.MoreObjects;
import io.ray.api.id.ActorId;

public final class WorkerCommitBarrierReport extends BaseWorkerCmd {

  public final long commitCheckpointId;

  public WorkerCommitBarrierReport(ActorId actorId, Long timestamp, long commitCheckpointId) {
    super(actorId, timestamp);
    this.commitCheckpointId = commitCheckpointId;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("commitCheckpointId", commitCheckpointId)
        .add("fromActorId", fromActorId)
        .add("timestamp", timestamp)
        .toString();
  }
}
