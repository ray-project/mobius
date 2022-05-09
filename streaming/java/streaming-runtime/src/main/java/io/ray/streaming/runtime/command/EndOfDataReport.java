package io.ray.streaming.runtime.command;

import io.ray.api.id.ActorId;

public final class EndOfDataReport extends BaseWorkerCmd {
  public EndOfDataReport(ActorId actorId, Long timestamp) {
    super(actorId, timestamp);
  }
}
