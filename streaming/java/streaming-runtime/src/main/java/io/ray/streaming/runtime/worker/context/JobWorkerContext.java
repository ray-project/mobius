package io.ray.streaming.runtime.worker.context;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import io.ray.api.ActorHandle;
import io.ray.api.BaseActorHandle;
import io.ray.api.id.ActorId;
import io.ray.runtime.actor.NativeJavaActorHandle;
import io.ray.streaming.common.config.CommonConfig;
import io.ray.streaming.common.enums.OperatorType;
import io.ray.streaming.common.serializer.KryoUtils;
import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;
import io.ray.streaming.runtime.generated.RemoteCall;
import io.ray.streaming.runtime.master.JobMaster;
import io.ray.streaming.runtime.python.GraphPbBuilder;
import io.ray.streaming.runtime.worker.JobWorkerType;
import io.ray.streaming.runtime.worker.tasks.ControlMessage;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * JobWorkerContext is part of execution vertex
 */
public final class JobWorkerContext implements Serializable {

  private ActorHandle masterActor;
  private ImmutableContext immutableContext;

  /**
   * The execution vertex info.
   */
  // TODO: please remove this field when kryo-removing is ready (yunye)
  private int executionVertexId;
  private byte[] vertexIdExecutionVertexMapBytes;
  private byte[] pythonWorkerContextBytes;

  private transient Map<Integer, ExecutionVertex> vertexIdExecutionVertexMap;

  /**
   * mark current worker context as changed
   */
  private Boolean isChanged = false;

  /**
   * The role(source/transform/sink) of current worker in changed sub dag.
   */
  private OperatorType roleInChangedSubDag = OperatorType.TRANSFORM;

  /**
   * Control messages.
   */
  private ArrayBlockingQueue<ControlMessage> mailbox = new ArrayBlockingQueue(16);

  public JobWorkerContext(ImmutableContext immutableContext) {
    this.immutableContext = Preconditions.checkNotNull(immutableContext);
    this.masterActor = immutableContext.getMasterActor();
  }

  public JobWorkerContext(ActorHandle<JobMaster> masterActor,
                          ExecutionVertex executionVertex,
                          byte[] vertexIdExecutionVertexMapBytes) {
    Preconditions.checkNotNull(masterActor);
    Preconditions.checkNotNull(vertexIdExecutionVertexMapBytes);
    this.masterActor = masterActor;
    this.executionVertexId = executionVertex.getExecutionVertexId();
    // bytes for serialized vertexIdExecutionVertexMap, we update it when buildWorkersContext
    this.vertexIdExecutionVertexMapBytes = vertexIdExecutionVertexMapBytes;
    buildPythonWorkerContextBytes(executionVertex);
  }

  private void buildPythonWorkerContextBytes(ExecutionVertex executionVertex) {
    if (executionVertex.getJobWorkerType() == JobWorkerType.PYTHON_WORKER) {
      pythonWorkerContextBytes = RemoteCall.PythonJobWorkerContext.newBuilder()
          .setWorkerId(String.valueOf(executionVertex.getExecutionVertexId()))
          .setMasterActor(ByteString
              .copyFrom((((NativeJavaActorHandle) getMasterActor()).toBytes())))
          .setExecutionVertexContext(
              new GraphPbBuilder().buildExecutionVertexContext(executionVertex))
          .setIsChanged(executionVertex.isChangedOrAffected())
          .build()
          .toByteArray();
    }
  }

  /**
   * Q:Why we need to init context in java worker?
   * A:Because we can skip context serialization in job master to avoid oom.
   */
  public void initContext() {
    if (this.vertexIdExecutionVertexMap == null || this.immutableContext == null) {
      ExecutionVertex executionVertex = resetAndGetExecutionVertex();
      buildExecutionVertex(executionVertex);
      this.immutableContext = new ImmutableContext(masterActor,
          getJobConf().get(CommonConfig.JOB_NAME),
          executionVertex.getExecutionJobVertexName(),
          executionVertex.getActorId(),
          executionVertex.getActorName());
    }
  }

  private ExecutionVertex buildExecutionVertex(ExecutionVertex executionVertex) {
    executionVertex.getInputEdges().forEach(executionEdge -> {
      executionEdge.setSource(getVertexIdExecutionVertexMap().get(executionEdge.getSourceVertexId()));
      executionEdge.setTarget(executionVertex);
    });
    executionVertex.getOutputEdges().forEach(executionEdge -> {
      executionEdge.setSource(executionVertex);
      executionEdge.setTarget(getVertexIdExecutionVertexMap().get(executionEdge.getTargetVertexId()));
    });
    executionVertex.getInputExecutionVertices().forEach(inputVertex -> {
      inputVertex.getOutputEdges().forEach(outExecutionEdge -> {
        outExecutionEdge.setSource(getVertexIdExecutionVertexMap().get(outExecutionEdge.getSourceVertexId()));
        outExecutionEdge.setTarget(getVertexIdExecutionVertexMap().get(outExecutionEdge.getTargetVertexId()));
      });
      inputVertex.getInputEdges().forEach(inExecutionEdge -> {
        inExecutionEdge.setSource(getVertexIdExecutionVertexMap().get(inExecutionEdge.getSourceVertexId()));
        inExecutionEdge.setTarget(getVertexIdExecutionVertexMap().get(inExecutionEdge.getTargetVertexId()));
      });
    });
    executionVertex.getOutputExecutionVertices().forEach(outputVertex -> {
      outputVertex.getInputEdges().forEach(inExecutionEdge -> {
        inExecutionEdge.setSource(getVertexIdExecutionVertexMap().get(inExecutionEdge.getSourceVertexId()));
        inExecutionEdge.setTarget(getVertexIdExecutionVertexMap().get(inExecutionEdge.getTargetVertexId()));
      });

      outputVertex.getOutputEdges().forEach(outExecutionEdge -> {
        outExecutionEdge.setSource(getVertexIdExecutionVertexMap().get(outExecutionEdge.getSourceVertexId()));
        outExecutionEdge.setTarget(getVertexIdExecutionVertexMap().get(outExecutionEdge.getTargetVertexId()));
      });

    });

    executionVertex.setRoleInChangedSubDag(this.roleInChangedSubDag);

    return executionVertex;
  }

  public int getExecutionVertexId() {
    return executionVertexId;
  }

  public void setExecutionVertexId(int executionVertexId) {
    this.executionVertexId = executionVertexId;
  }

  public ExecutionVertex getExecutionVertex() {
    if (vertexIdExecutionVertexMap == null) {
      vertexIdExecutionVertexMap = KryoUtils.readFromByteArray(vertexIdExecutionVertexMapBytes);
      return buildExecutionVertex(vertexIdExecutionVertexMap.get(executionVertexId));
    }
    return vertexIdExecutionVertexMap.get(executionVertexId);
  }

  public Map<Integer, ExecutionVertex> getVertexIdExecutionVertexMap() {
    Preconditions.checkArgument(vertexIdExecutionVertexMap != null,
        "Failed to get vertex map for it is empty.");
    return vertexIdExecutionVertexMap;
  }

  public ExecutionVertex resetAndGetExecutionVertex() {
    Preconditions.checkArgument(vertexIdExecutionVertexMapBytes != null,
        "Failed to get vertex map from bytes for it is empty.");
    vertexIdExecutionVertexMap = KryoUtils.readFromByteArray(vertexIdExecutionVertexMapBytes);
    ExecutionVertex executionVertex = vertexIdExecutionVertexMap.get(executionVertexId);
    buildExecutionVertex(executionVertex);
    return executionVertex;
  }

  public boolean isChanged() {
    return isChanged;
  }

  public void markAsChanged() {
    isChanged = true;
  }

  public void markAsUnchanged() {
    isChanged = false;
  }

  public OperatorType getRoleInChangedSubDag() {
    return roleInChangedSubDag;
  }

  public void setRoleInChangedSubDag(
      OperatorType roleInChangedSubDag) {
    this.roleInChangedSubDag = roleInChangedSubDag;
  }

  public ImmutableContext getImmutableContext() {
    return immutableContext;
  }

  public void setImmutableContext(ImmutableContext immutableContext) {
    this.immutableContext = immutableContext;
  }

  public ActorHandle<JobMaster> getMasterActor() {
    if (masterActor != null) {
      return masterActor;
    } else {
      Preconditions.checkArgument(immutableContext != null,
          "Failed to get master actor for immutable context is empty.");
      return immutableContext.getMasterActor();
    }
  }

  public Map<String, String> getJobConf() {
    return getExecutionVertex().getJobConfig();
  }

  public String getJobName() {
    if (immutableContext != null) {
      return immutableContext.getJobName();
    }
    return "";
  }

  public String getOpName() {
    if (immutableContext != null) {
      return immutableContext.getOpName();
    }
    return "";
  }

  public String getWorkerName() {
    if (immutableContext != null) {
      return immutableContext.getWorkerName();
    }
    return "";
  }

  public ActorId getWorkerActorId() {
    if (immutableContext != null) {
      return immutableContext.getActorId();
    }
    return null;
  }

  public Map<ActorId, String> getInputQueues() {
    return getExecutionVertex().getInputQueues();
  }

  public Map<ActorId, String> getOutputQueues() {
    return getExecutionVertex().getOutputQueues();
  }

  public Map<String, BaseActorHandle> getInputActors() {
    return getExecutionVertex().getInputActors();
  }

  public Map<String, BaseActorHandle> getOutputActors() {
    return getExecutionVertex().getOutputActors();
  }

  public Map<String, Boolean> getOutputCyclic() {
    return getExecutionVertex().getOutputCyclic();
  }

  public Map<String, Boolean> getInputCyclic() {
    return getExecutionVertex().getInputCyclic();
  }

  public ArrayBlockingQueue<ControlMessage> getMailbox() {
    return mailbox;
  }

  public void setMailbox(
      ArrayBlockingQueue<ControlMessage> mailbox) {
    this.mailbox = mailbox;
  }

  public void clearMailbox() {
    if (mailbox != null) {
      mailbox.clear();
    }
  }

  public byte[] getPythonWorkerContextBytes() {
    Preconditions.checkArgument(pythonWorkerContextBytes != null,
        "Python worker context bytes is empty.");
    return pythonWorkerContextBytes;
  }

  @Override
  public String toString() {
    if (immutableContext == null) {
      return MoreObjects.toStringHelper(this)
          .add("vertexId", executionVertexId)
          .toString();
    }
    return MoreObjects.toStringHelper(this)
        .add("jobName", getJobName())
        .add("opName", getOpName())
        .add("workerName", getWorkerName())
        .add("immutableContext", immutableContext)
        .add("workerId", getWorkerActorId())
        .add("inputActorQueues", getInputQueues())
        .add("outputActorQueues", getOutputQueues())
        .add("inputActors", getInputActors())
        .add("outputActors", getOutputActors())
        .add("isChanged", isChanged)
        .add("roleInChangedSubDag", roleInChangedSubDag)
        .add("mailbox", mailbox)
        .toString();
  }

}
