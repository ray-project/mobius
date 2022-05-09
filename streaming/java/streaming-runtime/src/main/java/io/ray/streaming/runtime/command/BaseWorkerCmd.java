package io.ray.streaming.runtime.command;

import io.ray.api.id.ActorId;
import java.io.Serializable;

public abstract class BaseWorkerCmd implements Serializable {

  public ActorId fromActorId;
  public Long timestamp;

  public BaseWorkerCmd() {
  }

  protected BaseWorkerCmd(ActorId actorId) {
    this.fromActorId = actorId;
    this.timestamp = System.currentTimeMillis();
  }
  protected BaseWorkerCmd(ActorId actorId, Long timestamp) {
    this.fromActorId = actorId;
    this.timestamp = timestamp;
  }

}
