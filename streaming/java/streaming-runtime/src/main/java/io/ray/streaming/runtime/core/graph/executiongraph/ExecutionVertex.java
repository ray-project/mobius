package io.ray.streaming.runtime.core.graph.executiongraph;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import io.ray.api.BaseActorHandle;
import io.ray.api.id.ActorId;
import io.ray.streaming.api.Language;
import io.ray.streaming.jobgraph.VertexType;
import io.ray.streaming.operator.StreamOperator;
import io.ray.streaming.runtime.config.master.ResourceConfig;
import io.ray.streaming.runtime.core.resource.ContainerId;
import io.ray.streaming.runtime.core.resource.ResourceType;
import io.ray.streaming.runtime.rpc.remoteworker.WorkerCaller;
import io.ray.streaming.runtime.transfer.channel.ChannelId;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/** Physical vertex, correspond to {@link ExecutionJobVertex}. */
public class ExecutionVertex implements Serializable {

  /** Unique id for execution vertex. */
  private final int executionVertexId;

  /** Immutable field inherited from {@link ExecutionJobVertex}. */
  private final int executionJobVertexId;

  private final String executionJobVertexName;
  private final StreamOperator streamOperator;
  private final VertexType vertexType;
  private final Language language;
  private final long buildTime;

  /** Resource used by ExecutionVertex. */
  private final Map<String, Double> resource;

  /** Parallelism of current vertex's operator. */
  private int parallelism;

  /**
   * Ordered sub index for execution vertex in a execution job vertex. Might be changed in dynamic
   * scheduling.
   */
  private int executionVertexIndex;

  private ExecutionVertexState state = ExecutionVertexState.TO_ADD;

  /** The id of the container which this vertex's worker actor belongs to. */
  private ContainerId containerId;

  private String pid;

  /** Worker actor handle. */
  private BaseActorHandle workerActor; // deprecated

  /**
   * For remote call of different type workers, like:
   * `JavaWorker`
   * `DynamicPyWorker`
   * `PythonWorker` i.e. python tide worker
   *  Here we use workerCaller to replace workerActor to make remote call more convenient.
   */
  private WorkerCaller workerCaller;

  /** Op config + job config. */
  private Map<String, String> workerConfig;

  private List<ExecutionEdge> inputEdges = new ArrayList<>();
  private List<ExecutionEdge> outputEdges = new ArrayList<>();

  private transient List<String> outputChannelIdList;
  private transient List<String> inputChannelIdList;

  private transient List<BaseActorHandle> outputActorList;
  private transient List<BaseActorHandle> inputActorList;
  private Map<Integer, String> exeVertexChannelMap;

  public ExecutionVertex(
      int globalIndex,
      int index,
      ExecutionJobVertex executionJobVertex,
      ResourceConfig resourceConfig) {
    this.executionVertexId = globalIndex;
    this.executionJobVertexId = executionJobVertex.getExecutionJobVertexId();
    this.executionJobVertexName = executionJobVertex.getExecutionJobVertexName();
    this.streamOperator = executionJobVertex.getStreamOperator();
    this.vertexType = executionJobVertex.getVertexType();
    this.language = executionJobVertex.getLanguage();
    this.buildTime = executionJobVertex.getBuildTime();
    this.parallelism = executionJobVertex.getParallelism();
    this.executionVertexIndex = index;
    this.resource = generateResources(resourceConfig);
    this.workerConfig = genWorkerConfig(executionJobVertex.getJobConfig());
  }

  private Map<String, String> genWorkerConfig(Map<String, String> jobConfig) {
    return new HashMap<>(jobConfig);
  }

  public int getExecutionVertexId() {
    return executionVertexId;
  }

  /**
   * Unique name generated by execution job vertex name and index of current execution vertex. e.g.
   * 1-ISourceOperator-3 (vertex index is 3)
   */
  public String getExecutionVertexName() {
    return executionJobVertexName + "-" + executionVertexIndex;
  }

  public int getExecutionJobVertexId() {
    return executionJobVertexId;
  }

  public String getExecutionJobVertexName() {
    return executionJobVertexName;
  }

  public StreamOperator getStreamOperator() {
    return streamOperator;
  }

  public VertexType getVertexType() {
    return vertexType;
  }

  public Language getLanguage() {
    return language;
  }

  public int getParallelism() {
    return parallelism;
  }

  public int getExecutionVertexIndex() {
    return executionVertexIndex;
  }

  public ExecutionVertexState getState() {
    return state;
  }

  public void setState(ExecutionVertexState state) {
    this.state = state;
  }

  public boolean isToAdd() {
    return ExecutionVertexState.TO_ADD.equals(state);
  }

  public boolean isRunning() {
    return ExecutionVertexState.RUNNING.equals(state);
  }

  public boolean isToUpdate() {
    return getState() == ExecutionVertexState.TO_ADD_RELATED
            || getState() == ExecutionVertexState.TO_DEL_RELATED
            || getState() == ExecutionVertexState.TO_UPDATE;
  }

  public boolean isToDelete() {
    return ExecutionVertexState.TO_DEL.equals(state);
  }

  public boolean isToChange() {
    return isToAdd() || isToDelete();
  }

  public boolean isEmptyWorkerCaller() {
    return workerCaller == null;
  }

  public BaseActorHandle getActor() {
    return workerActor;
  }

  public WorkerCaller getWorkerCaller() {
    Preconditions.checkNotNull(workerCaller, getActorFullName() + "'s worker caller is empty.");
    return workerCaller;
  }

  public String getActorFullName() {
    return getExecutionVertexName() + "|" + executionVertexId;
  }

  public void setActor(BaseActorHandle workerActor) {
    this.workerActor = workerActor;
  }

  public List<ExecutionEdge> getInputEdges() {
    return inputEdges;
  }

  public void setInputEdges(List<ExecutionEdge> inputEdges) {
    this.inputEdges = inputEdges;
  }

  public List<ExecutionEdge> getOutputEdges() {
    return outputEdges;
  }

  public void setOutputEdges(List<ExecutionEdge> outputEdges) {
    this.outputEdges = outputEdges;
  }

  public List<ExecutionVertex> getInputExecutionVertices() {
    return inputEdges.stream()
        .map(ExecutionEdge::getSource)
        .collect(Collectors.toList());
  }

  public List<ExecutionVertex> getOutputExecutionVertices() {
    return outputEdges.stream()
        .map(ExecutionEdge::getTarget)
        .collect(Collectors.toList());
  }

  public ActorId getActorId() {
    return null == workerActor ? null : workerActor.getId();
  }

  public String getActorName() {
    return String.valueOf(executionVertexId);
  }

  public Map<String, Double> getResource() {
    return resource;
  }

  public Map<String, String> getWorkerConfig() {
    return workerConfig;
  }

  public long getBuildTime() {
    return buildTime;
  }

  public ContainerId getContainerId() {
    return containerId;
  }

  public void setContainerId(ContainerId containerId) {
    this.containerId = containerId;
  }

  public String getPid() {
    return pid;
  }

  public void setPid(String pid) {
    this.pid = pid;
  }

  public void setContainerIfNotExist(ContainerId containerId) {
    if (null == this.containerId) {
      this.containerId = containerId;
    }
  }

  /*---------channel-actor relations---------*/
  public List<String> getOutputChannelIdList() {
    if (outputChannelIdList == null) {
      generateActorChannelInfo();
    }
    return outputChannelIdList;
  }

  public List<BaseActorHandle> getOutputActorList() {
    if (outputActorList == null) {
      generateActorChannelInfo();
    }
    return outputActorList;
  }

  public List<String> getInputChannelIdList() {
    if (inputChannelIdList == null) {
      generateActorChannelInfo();
    }
    return inputChannelIdList;
  }

  public List<BaseActorHandle> getInputActorList() {
    if (inputActorList == null) {
      generateActorChannelInfo();
    }
    return inputActorList;
  }

  public String getChannelIdByPeerVertex(ExecutionVertex peerVertex) {
    if (exeVertexChannelMap == null) {
      generateActorChannelInfo();
    }
    return exeVertexChannelMap.get(peerVertex.getExecutionVertexId());
  }

  private void generateActorChannelInfo() {
    inputChannelIdList = new ArrayList<>();
    inputActorList = new ArrayList<>();
    outputChannelIdList = new ArrayList<>();
    outputActorList = new ArrayList<>();
    exeVertexChannelMap = new HashMap<>();

    List<ExecutionEdge> inputEdges = getInputEdges();
    for (ExecutionEdge edge : inputEdges) {
      String channelId =
          ChannelId.genIdStr(
              edge.getSource().getExecutionVertexId(),
              getExecutionVertexId(),
              getBuildTime());
      inputChannelIdList.add(channelId);
      inputActorList.add(edge.getSource().getActor());
      exeVertexChannelMap.put(edge.getSource().getExecutionVertexId(), channelId);
    }

    List<ExecutionEdge> outputEdges = getOutputEdges();
    for (ExecutionEdge edge : outputEdges) {
      String channelId =
          ChannelId.genIdStr(
              getExecutionVertexId(),
              edge.getTarget().getExecutionVertexId(),
              getBuildTime());
      outputChannelIdList.add(channelId);
      outputActorList.add(edge.getTarget().getActor());
      exeVertexChannelMap.put(edge.getTarget().getExecutionVertexId(), channelId);
    }
  }

  private Map<String, Double> generateResources(ResourceConfig resourceConfig) {
    Map<String, Double> resourceMap = new HashMap<>();
    if (resourceConfig.isTaskCpuResourceLimit()) {
      resourceMap.put(ResourceType.CPU.name(), resourceConfig.taskCpuResource());
    }
    if (resourceConfig.isTaskMemResourceLimit()) {
      resourceMap.put(ResourceType.MEM.name(), resourceConfig.taskMemResource());
    }
    return resourceMap;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ExecutionVertex) {
      return this.executionVertexId == ((ExecutionVertex) obj).getExecutionVertexId();
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(executionVertexId, outputEdges);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("id", executionVertexId)
        .add("name", getExecutionVertexName())
        .add("resources", resource)
        .add("state", state)
        .add("containerId", containerId)
        .add("workerActor", workerActor)
        .toString();
  }
}
