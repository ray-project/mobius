package io.ray.streaming.runtime.worker.context;

import com.google.common.base.MoreObjects;
import io.ray.api.ActorHandle;
import io.ray.api.id.ActorId;
import io.ray.streaming.runtime.master.JobMaster;
import java.io.Serializable;

/**
 * ImmutableContext is the static context of `JobWorker` and will not change.
 */
public class ImmutableContext implements Serializable {

  private ActorHandle<JobMaster> masterActor;
  private String jobName;
  private String opName;
  private ActorId actorId;
  private String workerName;

  public ImmutableContext() {
  }

  public ImmutableContext(ActorHandle<JobMaster> masterActor, String jobName, String opName,
                          ActorId actorId, String workerName) {
    this.masterActor = masterActor;
    this.jobName = jobName;
    this.opName =opName;
    this.actorId = actorId;
    this.workerName = workerName;
  }

  public ActorHandle<JobMaster> getMasterActor() {
    return masterActor;
  }

  public void setMasterActor(
      ActorHandle<JobMaster> masterActor) {
    this.masterActor = masterActor;
  }

  public String getJobName() {
    return jobName;
  }

  public void setJobName(String jobName) {
    this.jobName = jobName;
  }

  public String getOpName() {
    return opName;
  }

  public void setOpName(String opName) {
    this.opName = opName;
  }

  public ActorId getActorId() {
    return actorId;
  }

  public void setActorId(ActorId actorId) {
    this.actorId = actorId;
  }

  public String getWorkerName() {
    return workerName;
  }

  public void setWorkerName(String workerName) {
    this.workerName = workerName;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("masterActor", masterActor)
        .add("jobName", jobName)
        .add("opName", opName)
        .add("actorId", actorId)
        .add("workerName", workerName)
        .toString();
  }
}