package io.ray.streaming.runtime.command;

import io.ray.api.id.ActorId;
import io.ray.streaming.runtime.core.command.BaseWorkerCmd;

public final class EndOfDataReport extends BaseWorkerCmd {
  public EndOfDataReport(ActorId actorId, Long timestamp) {
    super(actorId, timestamp);
  }
}
