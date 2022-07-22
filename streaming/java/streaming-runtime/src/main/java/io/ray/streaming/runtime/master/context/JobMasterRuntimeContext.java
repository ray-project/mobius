package io.ray.streaming.runtime.master.context;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Sets;
import io.ray.api.ActorHandle;
import io.ray.streaming.jobgraph.JobGraph;
import io.ray.streaming.runtime.command.EndOfDataReport;
import io.ray.streaming.runtime.config.StreamingConfig;
import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionGraph;
import io.ray.streaming.runtime.master.coordinator.command.BaseWorkerCmd;
import io.ray.streaming.runtime.master.joblifecycle.JobStatus;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Runtime context for job master, which will be stored in backend when saving checkpoint.
 *
 * <p>Including: graph, resource, checkpoint info, etc.
 */
public class JobMasterRuntimeContext implements Serializable {

  /*--------------Checkpoint----------------*/
  public volatile List<Long> checkpointIds = new ArrayList<>();
  public volatile long lastCheckpointId = 0;
  public volatile long lastCpTimestamp = 0;
  public volatile BlockingQueue<BaseWorkerCmd> cpCmds = new LinkedBlockingQueue<>();

  /*--------------Failover----------------*/
  public volatile BlockingQueue<BaseWorkerCmd> foCmds = new ArrayBlockingQueue<>(8192);
  public volatile Set<BaseWorkerCmd> unfinishedFoCmds = Sets.newConcurrentHashSet();

  /*--------------Lifecycle----------------*/
  public volatile BlockingQueue<EndOfDataReport> lifeCycleCmds = new ArrayBlockingQueue<>(8192);

  private volatile JobStatus jobStatus = JobStatus.SUBMITTING;

  private StreamingConfig config;
  private JobGraph jobGraph;
  private volatile ExecutionGraph executionGraph;

  /**
   * The JobMaster ActorHandle can be used to get its actor id, which can be further used to migrate
   * actor, build downstream contexts, i.e. {@link
   * io.ray.streaming.runtime.worker.context.JobWorkerContext}, etc.
   */
  private ActorHandle jobMasterActor;

  public JobMasterRuntimeContext(StreamingConfig config) {
    this.config = config;
  }

  public ActorHandle getJobMasterActor() {
    return jobMasterActor;
  }

  public void setJobMasterActor(ActorHandle jobMasterActor) {
    this.jobMasterActor = jobMasterActor;
  }

  public String getJobName() {
    return config.getMasterConfig().commonConfig.jobName();
  }

  public StreamingConfig getConfig() {
    return config;
  }

  public JobGraph getJobGraph() {
    return jobGraph;
  }

  public void setJobGraph(JobGraph jobGraph) {
    this.jobGraph = jobGraph;
  }

  public ExecutionGraph getExecutionGraph() {
    return executionGraph;
  }

  public void setExecutionGraph(ExecutionGraph executionGraph) {
    this.executionGraph = executionGraph;
  }

  public Long getLastValidCheckpointId() {
    if (checkpointIds.isEmpty()) {
      // OL is invalid checkpoint id, worker will pass it
      return 0L;
    }
    return checkpointIds.get(checkpointIds.size() - 1);
  }

  public JobStatus getJobStatus() {
    return jobStatus;
  }

  public synchronized void setJobStatus(JobStatus jobStatus) {
    this.jobStatus = jobStatus;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("jobGraph", jobGraph)
        .add("executionGraph", executionGraph)
        .add("jobStatus", jobStatus)
        .add("conf", config.getMap())
        .toString();
  }
}
