package io.ray.streaming.runtime.core.graph.executiongraph;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.ray.api.BaseActorHandle;
import io.ray.api.id.ActorId;
import io.ray.streaming.common.enums.OperatorType;
import io.ray.streaming.common.tuple.Tuple2;
import io.ray.streaming.runtime.core.graph.JobInformation;
import io.ray.streaming.runtime.master.scheduler.ActorRoleType;
import io.ray.streaming.runtime.master.scheduler.ExecutionGroup;
import io.ray.streaming.runtime.rpc.remoteworker.WorkerCaller;
import io.ray.streaming.runtime.util.GraphBuilder;
import io.ray.streaming.runtime.util.Serializer;
import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ExecutionGraph is the physical plan for scheduling。
 *
 * <p>
 * Structure(example):
 * ExecutionJobVertex1 - ExecutionJobEdge - ExecutionJobVertex2 - ExecutionJobEdge - ExecutionJobVertex3
 * |                   |                     |                    |                   |
 * ExecutionEdge_2-1   ExecutionVertex2-1  ExecutionEdge_3-1+3_2 ExecutionVertex3-1
 * ExecutionVertex1-1   ExecutionEdge_2-2   ExecutionVertex2-2  ExecutionEdge_3-1+3_2 ExecutionVertex3-2
 * ExecutionEdge_2-3   ExecutionVertex2-3  ExecutionEdge_3-1+3_2
 * </p>
 */
public class ExecutionGraph implements Serializable, Cloneable {

  private static final Logger LOG = LoggerFactory.getLogger(ExecutionGraph.class);

  private JobInformation jobInformation;
  private int maxParallelism;
  private Map<Integer, ExecutionJobVertex> executionJobVertices;
  private List<ExecutionJobVertex> verticesInCreationOrder;
  private List<ExecutionJobEdge> executionJobEdges;
  private List<Tuple2<ExecutionJobVertex, ExecutionJobVertex>> reversedTopologyOrderJobVertexPairs =
          Lists.newLinkedList();
  private AtomicInteger lastExecutionVertexIndex = new AtomicInteger(0);
  private final long buildTime = System.currentTimeMillis();

  // Those fields will be initialized after job worker context were built
  private Map<String, Set<BaseActorHandle>> queueActorsMap = Maps.newHashMap();
  private Map<ActorId, ExecutionVertex> actorIdExecutionVertexMap = Maps.newHashMap();
  private Map<Integer, ExecutionVertex> vertexIdExecutionVertexMap = Maps.newHashMap();
  private Map<BaseActorHandle, Integer> topoLevelOrder = Maps.newHashMap();
  private String digraph;

  private List<IndependentVertex> independentVertices = new ArrayList<>();

  private List<ExecutionGroup> executionGroups;

  public void attachExecutionJobVertex() {
    Preconditions.checkArgument(executionJobVertices != null && !executionJobVertices.isEmpty(),
            "execution job vertices are empty");
    executionJobVertices.values().forEach(executionJobVertex -> {
      executionJobVertex.connectInputs(
              getInputEdgesByExecutionJobVertexId(executionJobVertex.getExecutionJobVertexId()));
      executionJobVertex.connectOutputs(
              getOutputEdgesByJobVertexId(executionJobVertex.getExecutionJobVertexId()));
    });
  }

  public List<ExecutionJobEdge> getInputEdgesByExecutionJobVertexId(int executionJobVertexId) {
    return executionJobEdges.stream()
            .filter(executionJobEdge -> executionJobEdge
                    .getTarget().getExecutionJobVertexId() == executionJobVertexId)
            .collect(Collectors.toList());
  }

  public List<ExecutionJobEdge> getOutputEdgesByJobVertexId(int jobVertexId) {
    return executionJobEdges.stream()
            .filter(executionJobEdge -> executionJobEdge
                    .getSource().getExecutionJobVertexId() == jobVertexId)
            .collect(Collectors.toList());
  }

  public void setJobInformation(JobInformation jobInformation) {
    this.jobInformation = jobInformation;
  }

  public void setExecutionJobVertices(Map<Integer, ExecutionJobVertex> executionJobVertices) {
    this.executionJobVertices = executionJobVertices;
  }

  public void setVerticesInCreationOrder(List<ExecutionJobVertex> verticesInCreationOrder) {
    this.verticesInCreationOrder = verticesInCreationOrder;
  }

  public List<ExecutionJobEdge> getExecutionJobEdges() {
    return executionJobEdges;
  }

  public void setExecutionJobEdges(
          List<ExecutionJobEdge> executionJobEdges) {
    this.executionJobEdges = executionJobEdges;
  }

  public JobInformation getJobInformation() {
    return jobInformation;
  }

  public int getMaxParallelism() {
    return maxParallelism;
  }

  public Map<Integer, ExecutionJobVertex> getExecutionJobVertices() {
    return executionJobVertices;
  }

  public List<ExecutionJobVertex> getVerticesInCreationOrder() {
    return verticesInCreationOrder;
  }

  public int incLastExecutionVertexIndex() {
    return lastExecutionVertexIndex.getAndIncrement();
  }

  public AtomicInteger getLastExecutionVertexIndex() {
    return lastExecutionVertexIndex;
  }

  public long getBuildTime() {
    return buildTime;
  }

  public String getDigraph() {
    return digraph;
  }

  public void setDigraph(String digraph) {
    this.digraph = digraph;
  }

  public Map<BaseActorHandle, Integer> getTopoLevelOrder() {
    return topoLevelOrder;
  }

  public void setQueueActorsMap(
          Map<String, Set<BaseActorHandle>> queueActorsMap) {
    this.queueActorsMap = queueActorsMap;
  }

  public void setActorIdExecutionVertexMap(
          Map<ActorId, ExecutionVertex> actorIdExecutionVertexMap) {
    this.actorIdExecutionVertexMap = actorIdExecutionVertexMap;
  }

  public Map<ActorId, ExecutionVertex> getActorIdExecutionVertexMap() {
    return this.actorIdExecutionVertexMap;
  }

  public Map<Integer, ExecutionVertex> getVertexIdExecutionVertexMap() {
    return vertexIdExecutionVertexMap;
  }

  public void setVertexIdExecutionVertexMap(
          Map<Integer, ExecutionVertex> vertexIdExecutionVertexMap) {
    this.vertexIdExecutionVertexMap = vertexIdExecutionVertexMap;
  }


  private List<ExecutionVertex> getExecutionVertices() {
    return executionJobVertices.values().stream()
            .flatMap(executionJobVertex -> executionJobVertex.getExecutionVertices().stream())
            .collect(Collectors.toList());
  }

  @Override
  public ExecutionGraph clone() {
    byte[] executionGraphBytes = Serializer.encode(this);
    ExecutionGraph clonedExecutionGraph = Serializer.decode(executionGraphBytes);
    Map<Integer, ExecutionVertex> vertexMap = clonedExecutionGraph.getVertexIdExecutionVertexMap();
    if (vertexMap.size() == 0) {
      getExecutionVertices().forEach(executionVertex -> {
        vertexMap.put(executionVertex.getExecutionVertexId(), executionVertex);
      });
    }
    for (ExecutionVertex entry : clonedExecutionGraph.getExecutionVertices()) {
      entry.getInputEdges().forEach(executionEdge -> {
        executionEdge.setSource(vertexMap.get(executionEdge.getSourceVertexId()));
        executionEdge.setTarget(vertexMap.get(executionEdge.getTargetVertexId()));
      });
      entry.getOutputEdges().forEach(executionEdge -> {
        executionEdge.setSource(vertexMap.get(executionEdge.getSourceVertexId()));
        executionEdge.setTarget(vertexMap.get(executionEdge.getTargetVertexId()));
      });
    }
    return clonedExecutionGraph;
  }

  public List<ExecutionVertex> getAllNewbornVertices() {
    List<ExecutionVertex> executionVertices = new ArrayList<>();
    getVerticesInCreationOrder().stream()
            .map(ExecutionJobVertex::getNewbornVertices)
            .forEach(executionVertices::addAll);
    return executionVertices;
  }

  public List<ExecutionVertex> getAllMoribundVertices() {
    List<ExecutionVertex> executionVertices = new ArrayList<>();
    getVerticesInCreationOrder().stream()
            .map(ExecutionJobVertex::getMoribundVertices)
            .forEach(executionVertices::addAll);
    return executionVertices;
  }

  public Map<String, String> getJobConf() {
    return jobInformation.getJobConf();
  }

  public Map<String, ExecutionVertex> genVertexMap() {
    return getAllExecutionVertices().stream()
            .collect(Collectors.toMap(
                    vertex -> vertex.getActorId().toString(),
                    vertex -> vertex,
                    (v1, v2) -> v1));
  }

  public Map<String, ExecutionJobVertex> genJobVertexMap() {
    return verticesInCreationOrder.stream()
            .collect(Collectors.toMap(
                    jobVertex -> jobVertex.getJobVertex().getOperatorNameWithIndex(),
                    jobVertex -> jobVertex,
                    (v1, v2) -> v1));
  }

  // ----------------------------------------------------------------------
  // Actor relation methods
  // ----------------------------------------------------------------------JobGraph

  public List<ExecutionJobVertex> getSourceJobVertices() {
    return executionJobVertices.values().stream()
            .filter(ExecutionJobVertex::isSourceVertex)
            .collect(Collectors.toList());
  }

  public List<ExecutionJobVertex> getSinkJobVertices() {
    return executionJobVertices.values().stream()
            .filter(ExecutionJobVertex::isSinkVertex)
            .collect(Collectors.toList());
  }

  public List<ExecutionJobVertex> getNonSourceJobVertices() {
    return executionJobVertices.values().stream()
            .filter(jobVertex -> !jobVertex.isSourceVertex())
            .collect(Collectors.toList());
  }

  public List<ExecutionJobVertex> getTransformJobVertices() {
    return executionJobVertices.values().stream()
            .filter(jobVertex -> !jobVertex.isSourceVertex() && !jobVertex.isSinkVertex())
            .collect(Collectors.toList());
  }

  public List<ExecutionJobVertex> getAllExecutionJobVertices() {
    return new ArrayList<>(executionJobVertices.values());
  }

  public List<ExecutionJobVertex> getChangedExecutionJobVertices() {
    return getAllExecutionJobVertices().stream()
            .filter(ExecutionJobVertex::isChanged)
            .collect(Collectors.toList());
  }

  public List<BaseActorHandle> getActorsByVertexName(String vertexName) {
    return executionJobVertices.values().stream()
            .filter(executionJobVertex ->
                vertexName.equals(executionJobVertex.getExecutionJobVertexName()))
            .map(ExecutionJobVertex::getAllActors)
            .flatMap(Collection::stream).collect(Collectors.toList());
  }

  public List<ExecutionVertex> getAllExecutionVertices() {
    return getAllExecutionJobVertices().stream()
            .map(ExecutionJobVertex::getExecutionVertices)
            .flatMap(Collection::stream)
            .collect(Collectors.toList());
  }

  public List<ExecutionEdge> getAllExecutionEdges() {
    return getExecutionJobEdges().stream()
            .map(ExecutionJobEdge::getExecutionEdges)
            .flatMap(Collection::stream)
            .collect(Collectors.toList());
  }

  public List<WorkerCaller> getWorkerCallersFromJobVertices(
          List<ExecutionJobVertex> executionJobVertices) {
    return executionJobVertices.stream()
            .map(ExecutionJobVertex::getExecutionVertices)
            .flatMap(Collection::stream)
            .filter(executionVertex -> !executionVertex.isEmptyWorkerCaller())
            .map(ExecutionVertex::getWorkerCaller)
            .collect(Collectors.toList());
  }

  public List<BaseActorHandle> getActorsFromJobVertices(
      List<ExecutionJobVertex> executionJobVertices) {
    return executionJobVertices.stream()
            .map(ExecutionJobVertex::getExecutionVertices)
            .flatMap(Collection::stream)
            .map(ExecutionVertex::getActor)
            .collect(Collectors.toList());
  }

  public ExecutionVertex getExecutionVertexByActorId(ActorId actorId) {
    return actorIdExecutionVertexMap.get(actorId);
  }

  public ExecutionVertex getExecutionVertexById(int execVertexId) {
    return executionJobVertices.values().stream()
            .map(ExecutionJobVertex::getExecutionVertices)
            .flatMap(Collection::stream)
            .filter(executionVertex -> execVertexId == executionVertex.getExecutionVertexId())
            .findFirst().orElse(null);
  }

  public List<ExecutionVertex> getExecutionVerticesById(List<Integer> execVertexIds) {
    return execVertexIds.stream()
            .map(this::getExecutionVertexById)
            .collect(Collectors.toList());
  }

  public ExecutionVertex getExecutionVertexByName(String execVertexName) {
    return executionJobVertices.values().stream()
            .map(ExecutionJobVertex::getExecutionVertices)
            .flatMap(Collection::stream)
            .filter(executionVertex -> execVertexName.equals(executionVertex.getExecutionVertexName()))
            .findFirst().orElse(null);
  }

  public List<ExecutionVertex> getExecutionVerticesByName(List<String> execVertexNames) {
    return execVertexNames.stream()
            .map(this::getExecutionVertexByName)
            .collect(Collectors.toList());
  }

  public ExecutionJobVertex getExecutionJobVertexByName(String execJobVertexName) {
    return executionJobVertices.values().stream()
            .filter(executionJobVertex ->
                execJobVertexName.equals(executionJobVertex.getExecutionJobVertexName()))
            .findFirst().orElse(null);
  }

  public List<ExecutionJobVertex> getDownStreamExecutionJobVerticesByName(String execJobVertexName) {
    ExecutionJobVertex executionJobVertex = getExecutionJobVertexByName(execJobVertexName);
    if (executionJobVertex != null) {
      return executionJobVertex.getOutputExecJobVertices();
    }
    return null;
  }

  public List<String> getDownStreamExecutionJobVerticesNamesByName(String execJobVertexName) {
    List<ExecutionJobVertex> downStreamExecutionJobVertices = getDownStreamExecutionJobVerticesByName(execJobVertexName);
    if (downStreamExecutionJobVertices != null) {
      return downStreamExecutionJobVertices.stream()
              .map(ExecutionJobVertex::getExecutionJobVertexName)
              .collect(Collectors.toList());
    }
    return Collections.emptyList();
  }

  public List<WorkerCaller> getSourceWorkerCallers() {
    return getWorkerCallersFromJobVertices(getSourceJobVertices());
  }

  public List<BaseActorHandle> getSourceActors() {
    return getActorsFromJobVertices(getSourceJobVertices());
  }

  public List<WorkerCaller> getSinkWorkerCallers() {
    return getWorkerCallersFromJobVertices(getSinkJobVertices());
  }

  public List<BaseActorHandle> getSinkActors() {
    return getActorsFromJobVertices(getSinkJobVertices());
  }

  public List<WorkerCaller> getNonSourceWorkerCallers() {
    return getWorkerCallersFromJobVertices(getNonSourceJobVertices());
  }

  public List<BaseActorHandle> getNonSourceActors() {
    return getActorsFromJobVertices(getNonSourceJobVertices());
  }

  public List<BaseActorHandle> getTransformActors() {
    return getActorsFromJobVertices(getTransformJobVertices());
  }

  public List<WorkerCaller> getAllWorkerCallers() {
    return getWorkerCallersFromJobVertices(getAllExecutionJobVertices());
  }

  public List<BaseActorHandle> getAllActors() {
    return getActorsFromJobVertices(getAllExecutionJobVertices());
  }

  public Map<ActorId, WorkerCaller> getSourceWorkerCallersMap() {
    final Map<ActorId, WorkerCaller> workerCallersMap = new HashMap<>();
    getSourceWorkerCallers().forEach(workerCaller ->
        workerCallersMap.put(workerCaller.getActorHandle().getId(), workerCaller));
    return Collections.unmodifiableMap(workerCallersMap);
  }

  public Map<ActorId, BaseActorHandle> getSourceActorsMap() {
    final Map<ActorId, BaseActorHandle> actorsMap = new HashMap<>();
    getSourceActors().forEach(actor -> actorsMap.put(actor.getId(), actor));
    return Collections.unmodifiableMap(actorsMap);
  }

  public Map<ActorId, WorkerCaller> getSinkWorkerCallersMap() {
    final Map<ActorId, WorkerCaller> workerCallersMap = new HashMap<>();
    getSinkWorkerCallers().forEach(
            workerCaller -> workerCallersMap.put(workerCaller.getActorHandle().getId(), workerCaller));
    return Collections.unmodifiableMap(workerCallersMap);
  }

  public Map<ActorId, BaseActorHandle> getSinkActorsMap() {
    final Map<ActorId, BaseActorHandle> actorsMap = new HashMap<>();
    getSinkActors().forEach(actor -> actorsMap.put(actor.getId(), actor));
    return Collections.unmodifiableMap(actorsMap);
  }

  public Map<ActorId, BaseActorHandle> getTransformActorsMap() {
    final Map<ActorId, BaseActorHandle> actorsMap = new HashMap<>();
    getTransformActors().forEach(actor -> actorsMap.put(actor.getId(), actor));
    return Collections.unmodifiableMap(actorsMap);
  }

  public List<ActorId> getAllActorsId() {
    return getAllActors().stream()
            .map(actor -> actor.getId())
            .collect(Collectors.toList());
  }

  /**
   * get all down stream actors. for example:
   * <pre>
   * actorA - actorB - actorD - actorF
   *          \
   *           actorC - actorE
   * </pre>
   * if para is actorA, we will only return actorB & actorC & actorD & actorE & actorF.
   *
   * @param actor actor
   * @return direct down stream actors
   */
  public Set<BaseActorHandle> getAllDownStreamActors(BaseActorHandle actor) {

    Queue<BaseActorHandle> queue = new ArrayDeque<>();
    queue.add(actor);

    Set<BaseActorHandle> actorSet = new HashSet<>();
    while (!queue.isEmpty()) {
      BaseActorHandle qActor = queue.poll();

      ExecutionVertex vertex = actorIdExecutionVertexMap.get(qActor.getId());
      if (vertex != null) {
        Set<BaseActorHandle> outputActors = vertex.getOutputExecutionVertices().stream()
                .map(outputVertex -> outputVertex.getActor())
                .collect(Collectors.toSet());
        queue.addAll(outputActors);
        actorSet.addAll(outputActors);
      }
    }

    return Collections.unmodifiableSet(actorSet);
  }

  public Set<BaseActorHandle> getActorsByQueueName(String queueName) {
    return queueActorsMap.getOrDefault(queueName, Sets.newHashSet());
  }

  public Set<String> getQueuesByActor(BaseActorHandle actor) {
    return queueActorsMap.entrySet().stream()
            .filter(entry -> entry.getValue().contains(actor))
            .map(entry -> entry.getKey())
            .collect(Collectors.toSet());
  }

  public String getQueueNameByActor(BaseActorHandle actor1, BaseActorHandle actor2) {
    // only for test, very slow, could be optimized
    final String[] res = new String[1];
    queueActorsMap.forEach((queue, actorSet) -> {
      if (actorSet.contains(actor1) && actorSet.contains(actor2)) {
        res[0] = queue;
      }
    });
    return res[0];
  }

  public BaseActorHandle getAnotherActor(BaseActorHandle qActor, String queueName) {
    Set<BaseActorHandle> set = getActorsByQueueName(queueName);
    final BaseActorHandle[] res = new BaseActorHandle[1];
    set.forEach(actor -> {
      if (!actor.equals(qActor)) {
        res[0] = actor;
      }
    });
    return res[0];
  }

  public int getTopoLevelOrder(BaseActorHandle actor) {
    return topoLevelOrder.get(actor);
  }

  public Optional<WorkerCaller> getWorkerCallerById(ActorId actorId) {
    return getAllWorkerCallers().stream()
            .filter(workerCaller -> workerCaller.getActorHandle().getId().equals(actorId)).findFirst();
  }

  public Optional<BaseActorHandle> getActorById(ActorId actorId) {
    return getAllActors().stream()
            .filter(actor -> actor.getId().equals(actorId))
            .findFirst();
  }

  public Set<String> getActorName(Set<ActorId> actorIds) {
    return getAllExecutionVertices().stream()
            .filter(executionVertex -> actorIds.contains(executionVertex.getActorId()))
            .map(executionVertex -> executionVertex.getActorName())
            .collect(Collectors.toSet());
  }

  public String getActorName(ActorId actorId) {
    Set<ActorId> set = Sets.newHashSet();
    set.add(actorId);
    Set<String> result = getActorName(set);
    if (result.isEmpty()) {
      return null;
    }
    return result.iterator().next();
  }

  public Map<String, String> getActorTags(ActorId actorId) {
    Map<String, String> actorTags = new HashMap<>();
    // TODO: delete origin tags
    actorTags.put("actor_id", actorId.toString());
    actorTags.put(ScopeFormat.REMOTE_WORKER_ID, actorId.toString());
    Optional<BaseActorHandle> rayActor = getActorById(actorId);
    if (rayActor.isPresent()) {
      ExecutionVertex executionVertex = getExecutionVertexByActorId(actorId);
      actorTags.put("op_name", executionVertex.getExecutionJobVertexName());
      actorTags.put("op_index", String.valueOf(executionVertex.getExecutionJobVertexId()));
      // TODO: delete origin tags
      actorTags.put(ScopeFormat.REMOTE_OP_NAME, executionVertex.getExecutionJobVertexName());
      actorTags.put(ScopeFormat.REMOTE_OP_INDEX,
              String.valueOf(executionVertex.getExecutionJobVertexId()));
    }
    return actorTags;
  }

  public Map<String, String> getActorTags(BaseActorHandle actor) {
    return getActorTags(actor.getId());
  }

  public List<WorkerCaller> getSubDagSourceWorkerCallers() {
    List<WorkerCaller> workerCallers = new ArrayList<>();
    this.getAllExecutionVertices().forEach(v -> {
      if ((v.getRoleInChangedSubDag() == OperatorType.SOURCE)
              || (v.getRoleInChangedSubDag() == OperatorType.SOURCE_AND_SINK)) {
        workerCallers.add(v.getWorkerCaller());
      }
    });
    return workerCallers;
  }

  public List<BaseActorHandle> getSubDagSources() {
    List<BaseActorHandle> subDagSources = new ArrayList<>();
    this.getAllExecutionVertices().forEach(v -> {
      if ((v.getRoleInChangedSubDag() == OperatorType.SOURCE)
              || (v.getRoleInChangedSubDag() == OperatorType.SOURCE_AND_SINK)) {
        subDagSources.add(v.getActor());
      }
    });
    return subDagSources;
  }

  public List<BaseActorHandle> getSubDagSinks() {
    List<BaseActorHandle> subDagSinks = new ArrayList<>();
    this.getAllExecutionVertices().forEach(v -> {
      if ((v.getRoleInChangedSubDag() == OperatorType.SINK)
              || (v.getRoleInChangedSubDag() == OperatorType.SOURCE_AND_SINK)) {
        subDagSinks.add(v.getActor());
      }
    });
    return subDagSinks;
  }

  public List<Tuple2<ExecutionJobVertex, ExecutionJobVertex>> getReversedTopologyOrderJobVertexPairs() {
    return reversedTopologyOrderJobVertexPairs;
  }

  public void setReversedTopologyOrderJobVertexPairs(
          List<Tuple2<ExecutionJobVertex, ExecutionJobVertex>> reversedTopologyOrderJobVertexPairs) {
    this.reversedTopologyOrderJobVertexPairs = reversedTopologyOrderJobVertexPairs;
  }

  public List<ExecutionJobVertex> getTopologyOrderJobVertex() {
    HashMap<ExecutionJobVertex, Integer> inMap = new HashMap<>();
    Queue<ExecutionJobVertex> zeroInQueue = new LinkedList<>();
    for (ExecutionJobVertex node : verticesInCreationOrder) {
      inMap.put(node, node.getInputExecJobVertices().size());
      if (node.getInputExecJobVertices().size() == 0) {
        zeroInQueue.add(node);
      }
    }
    List<ExecutionJobVertex> topologyOrderJobVertex = Lists.newLinkedList();
    while (!zeroInQueue.isEmpty()) {
      ExecutionJobVertex cur = zeroInQueue.poll();
      topologyOrderJobVertex.add(cur);
      for (ExecutionJobVertex next : cur.getOutputExecJobVertices()) {
        inMap.put(next, inMap.get(next) - 1);
        if (inMap.get(next) == 0) {
          zeroInQueue.add(next);
        }
      }
    }
    return topologyOrderJobVertex;
  }

  public String getDigest() {
    GraphBuilder graphBuilder = new GraphBuilder();

    getAllExecutionVertices().forEach(curVertex -> {
      // current
      actorIdExecutionVertexMap.put(curVertex.getActorId(), curVertex);

      // input
      List<ExecutionEdge> inputEdges = curVertex.getInputEdges();
      inputEdges.stream().filter(ExecutionEdge::isAlive).forEach(inputEdge -> {
        ExecutionVertex inputVertex = inputEdge.getSource();
        graphBuilder.append(
                inputVertex.getExecutionVertexName(),
                curVertex.getExecutionVertexName(),
                inputEdge.getExecutionEdgeName()
        );
      });

      // isolated node
      if (curVertex.getInputQueues().isEmpty() &&
              curVertex.getOutputActors().isEmpty()) {
        graphBuilder.append(curVertex.getExecutionVertexName());
      }
    });

    return graphBuilder.build();
  }

  public List<IndependentVertex> getIndependentVertices() {
    return independentVertices;
  }

  public List<IndependentVertex> getSpecifiedTypeIndependentActors(String actorRoleType) {
    return independentVertices.stream()
            .filter(actor -> actor.getRoleName() == ActorRoleType.valueOfNameOrDesc(actorRoleType))
            .collect(Collectors.toList());
  }

  public List<IndependentVertex> getSpecifiedTypeIndependentActors(ActorRoleType actorRoleType) {
    return independentVertices.stream()
            .filter(actor -> actor.getRoleName() == actorRoleType)
            .collect(Collectors.toList());
  }

  public void clearIndependentActors() {
    independentVertices.clear();
  }

  public List<TrainingVertex> getTrainingActors() {
    return independentVertices.stream()
            .filter(actor -> actor.getRoleName() == ActorRoleType.PARAMETER_SERVER
                    || actor.getRoleName() == ActorRoleType.EVALUATOR )
            .map(actor -> (TrainingVertex)actor)
            .collect(Collectors.toList());
  }

  public List<ParameterServerVertex> getParameterServerActors() {
    return independentVertices.stream()
            .filter(actor -> actor.getRoleName() == ActorRoleType.PARAMETER_SERVER)
            .map(actor -> (ParameterServerVertex)actor)
            .collect(Collectors.toList());
  }

  public List<EvaluatorVertex> getEvaluatorActors() {
    return independentVertices.stream()
            .filter(actor -> actor.getRoleName() == ActorRoleType.EVALUATOR)
            .map(actor -> (EvaluatorVertex)actor)
            .collect(Collectors.toList());
  }

  public void addIndependentVertices(
          List<? extends IndependentVertex> independentActors) {
    this.independentVertices.addAll(independentActors);
  }

  public int getIndependentActorSize() {
    return independentVertices.size();
  }

  public int getTotalDAGActorNum() {
    return getAllExecutionVertices().size();
  }

  public int getTotalActorNum() {
    return getAllExecutionVertices().size() + getIndependentActorSize();
  }

  public List<ExecutionGroup> getExecutionGroups() {
    return executionGroups;
  }

  public ExecutionGroup getExecutionGroupById(int groupId) {
    return executionGroups.stream()
            .filter(executionGroup -> executionGroup.getGroupId() == groupId)
            .findFirst().orElse(null);
  }

  public void setExecutionGroups(List<ExecutionGroup> executionGroups) {
    this.executionGroups = executionGroups;
  }

  public void buildPlacementGroupToAllVertices() {
    if (executionGroups != null) {
      executionGroups.stream()
              .sorted(Comparator.comparing(ExecutionGroup::getGroupId))
              .forEach(executionGroup -> {
                executionGroup.buildPlacementGroup(getJobConf());
                executionGroup.getBundles().forEach(executionBundle -> {
                  getExecutionVertexById(executionBundle.getId()).setExecutionBundle(executionBundle);
                });
              });
    }
  }

  public void refreshExecutionGroups() {
    if (executionGroups != null) {
      executionGroups.forEach(ExecutionGroup::refresh);
      executionGroups.removeIf(executionGroup -> executionGroup.getResourceState() == ResourceState.TO_RELEASE);
    }
    LOG.info("Execution groups after refresh: {}.", executionGroups);
  }

  public void removePlacementGroupToAllVertices() {
    if (executionGroups != null) {
      executionGroups.stream()
              .filter(group -> group.getPlacementGroup() != null)
              .forEach(ExecutionGroup::removePlacementGroup);
    }
    getAllExecutionVertices().forEach(executionVertex -> executionVertex.setExecutionBundle(null));
    executionGroups = null;
    LOG.info("All the placement groups have been removed.");
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
            .add("digraph", digraph)
            .add("independentVertices", independentVertices)
            .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ExecutionGraph that = (ExecutionGraph) o;
    return getDigest().equals(that.getDigest());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getDigest());
  }
}
