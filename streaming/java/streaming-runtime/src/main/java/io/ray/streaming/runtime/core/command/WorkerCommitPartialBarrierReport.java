package io.ray.streaming.runtime.core.command;

import com.google.common.base.MoreObjects;
import io.ray.api.id.ActorId;

public class WorkerCommitPartialBarrierReport extends BaseWorkerCmd {

  public final long globalCheckpointId;
  public final long partialCheckpointId;

  public WorkerCommitPartialBarrierReport(final ActorId actorId, final long globalCheckpointId,
                                          final long partialCheckpointId) {
    super(actorId);
    this.globalCheckpointId = globalCheckpointId;
    this.partialCheckpointId = partialCheckpointId;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("globalCheckpointId", globalCheckpointId)
        .add("partialCheckpointId", partialCheckpointId)
        .add("fromActorId", fromActorId)
        .toString();
  }
}
