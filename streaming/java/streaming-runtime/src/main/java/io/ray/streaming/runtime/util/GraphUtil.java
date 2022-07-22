package io.ray.streaming.runtime.util;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import io.ray.streaming.common.tuple.Tuple2;
import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionGraph;
import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionJobVertex;
import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;
import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertexState;
import io.ray.streaming.runtime.core.graph.executiongraph.IndependentExecutionVertex;
import io.ray.streaming.runtime.master.scheduler.ExecutionBundle;
import io.ray.streaming.runtime.master.scheduler.ExecutionGroup;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

public class GraphUtil {

  private static final Logger LOG = LoggerFactory.getLogger(GraphUtil.class);

  public static final String OP_NAME_DELIMITER = "-";

  public static void buildExecutionEdges(ExecutionGraph executionGraph) {
    Map<Integer, ExecutionVertex> vertexMap =
        executionGraph.getExecutionVertexIdExecutionVertexMap();
    executionGraph
        .getAllExecutionEdges()
        .forEach(
            executionEdge -> {
              ExecutionVertex source = vertexMap.get(executionEdge.getSourceVertexId());
              ExecutionVertex target = vertexMap.get(executionEdge.getTargetVertexId());
              executionEdge.setSource(source);
              executionEdge.setTarget(target);
            });
  }

  /**
   * Return vertices that source > target.
   *
   * @param source source executionGraph
   * @param target target executionGraph
   * @return vertices
   */
  public static List<ExecutionVertex> diffExecutionGraphForAddedVertices(
      ExecutionGraph source, ExecutionGraph target) {
    List<ExecutionVertex> addedVertices = new ArrayList<>();

    for (ExecutionJobVertex sourceExecutionJobVertex :
        source.getJobVertexIdExecutionJobVertexMap().values()) {
      ExecutionJobVertex targetExecutionJobVertex =
          target
              .getJobVertexIdExecutionJobVertexMap()
              .get(sourceExecutionJobVertex.getJobVertex().getVertexId());
      List<ExecutionVertex> sourceExecutionVertices =
          sourceExecutionJobVertex.getExecutionVertices();

      // add all if target execution job vertex does not exist
      if (targetExecutionJobVertex == null) {
        addedVertices.addAll(sourceExecutionVertices);
        continue;
      }

      int diffSize =
          sourceExecutionVertices.size() - targetExecutionJobVertex.getExecutionVertices().size();
      if (diffSize > 0) {
        for (int index = sourceExecutionVertices.size() - 1; diffSize > 0; diffSize--, index--) {
          addedVertices.add(sourceExecutionVertices.get(index));
        }
      } else {
        continue;
      }
    }

    return addedVertices;
  }

  /**
   * Return vertices that source < target.
   *
   * @param source source executionGraph
   * @param target target executionGraph
   * @return vertices
   */
  public static List<ExecutionVertex> diffExecutionGraphForDeletedVertices(
      ExecutionGraph source, ExecutionGraph target) {
    List<ExecutionVertex> deletedVertices = new ArrayList<>();

    for (ExecutionJobVertex targetExecutionJobVertex :
        target.getJobVertexIdExecutionJobVertexMap().values()) {
      ExecutionJobVertex sourceExecutionJobVertex =
          source
              .getJobVertexIdExecutionJobVertexMap()
              .get(targetExecutionJobVertex.getJobVertex().getVertexId());
      List<ExecutionVertex> targetExecutionVertices =
          targetExecutionJobVertex.getExecutionVertices();

      // add all if source execution job vertex does not exist
      if (sourceExecutionJobVertex == null) {
        deletedVertices.addAll(targetExecutionVertices);
        continue;
      }

      int diffSize =
          targetExecutionVertices.size() - sourceExecutionJobVertex.getExecutionVertices().size();
      if (diffSize > 0) {
        for (int index = targetExecutionVertices.size() - 1; diffSize > 0; diffSize--, index--) {
          deletedVertices.add(targetExecutionVertices.get(index));
        }
      } else {
        continue;
      }
    }

    return deletedVertices;
  }

  /**
   * Sum total parallelism from job graph.
   *
   * @param jobGraph job graph
   * @return total parallelism
   */
  public static int calculateActualParallelism(io.ray.streaming.jobgraph.JobGraph jobGraph) {
    return jobGraph.getJobVertices().stream()
        .mapToInt(io.ray.streaming.jobgraph.JobVertex::getParallelism)
        .sum();
  }

  /**
   * Calculate the minimum parallelism of all the operators.
   *
   * @param executionGraph target graph
   * @return the minimum parallelism
   */
  public static int getMinParallelism(ExecutionGraph executionGraph) {
    int minParallelism =
        executionGraph.getAllExecutionJobVertices().stream()
            .mapToInt(ExecutionJobVertex::getParallelism)
            .min()
            .orElse(0);

    Preconditions.checkArgument(minParallelism != 0, "get 0 parallelism abnormally");
    return minParallelism;
  }

  /**
   * Generate operator's worker actor name map. e.g.{1=[1-src-0|0], 2=[2-filter-0|1], 3=[3-sink-1|3,
   * 3-sink-0|2]}
   *
   * @param executionGraph key: ExecutionJobVertex ID, value: set of worker actors' full name
   * @return map result
   */
  public static Map<String, Set<String>> generateOperatorActorNameMap(
      ExecutionGraph executionGraph) {
    if (executionGraph == null) {
      return Collections.emptyMap();
    }
    Map<String, Set<String>> resultMap =
        new HashMap<>(executionGraph.getJobVertexIdExecutionJobVertexMap().size());

    // for dag
    executionGraph
        .getAllExecutionJobVertices()
        .forEach(
            executionJobVertex -> {
              Set<String> actorNameSet = new HashSet<>(executionJobVertex.getParallelism());
              for (ExecutionVertex executionVertex : executionJobVertex.getExecutionVertices()) {
                actorNameSet.add(executionVertex.getActorFullName());
              }
              resultMap.put(
                  String.valueOf(executionJobVertex.getExecutionJobVertexId()), actorNameSet);
            });

    // for independent
    executionGraph
        .getIndependentVertices()
        .forEach(
            independentExecutionVertex -> {
              String keyName = independentExecutionVertex.getActorName();
              String actorName = independentExecutionVertex.getName();
              resultMap.computeIfAbsent(keyName, value -> new HashSet<>()).add(actorName);
            });

    return resultMap;
  }

  /**
   * Generate operator's worker actor name json.
   * e.g.{"1":["1-src-0|0"],"2":["2-filter-0|1"],"3":["3-sink-1|3","3-sink-0|2"]}
   *
   * @param executionGraph graph
   * @return json result
   */
  public static String generateOperatorActorNameJson(ExecutionGraph executionGraph) {
    return new Gson().toJson(generateOperatorActorNameMap(executionGraph));
  }

  /**
   * Get changed vertices from graph according to the specified state
   *
   * @param executionGraph graph
   * @param targetState the specified state
   * @return vertices
   */
  public static List<ExecutionVertex> getSpecifiedChangedExecutionVertices(
      ExecutionGraph executionGraph, ExecutionVertexState targetState) {
    return executionGraph.getChangedExecutionJobVertices().stream()
        .map(ExecutionJobVertex::getExecutionVertices)
        .flatMap(Collection::stream)
        .filter(executionVertex -> executionVertex.getState() == targetState)
        .collect(Collectors.toList());
  }

  /** Find execution vertex's bundle */
  public static ExecutionBundle findBundleForExecutionVertexById(
      ExecutionGraph executionGraph, int id) {
    return executionGraph.getExecutionGroups().stream()
        .map(ExecutionGroup::getBundles)
        .flatMap(Collection::stream)
        .filter(bundle -> bundle.getId() == id)
        .findFirst()
        .orElse(null);
  }

  /**
   * Generate limit parallelism map according to the expect and factor.
   *
   * @param expectParallelism expect
   * @param factor factor
   * @return limit parallelism map
   */
  public static Map<String, Tuple2<Integer, Integer>> generateLimitParallelism(
      Map<String, Integer> expectParallelism, float factor) {
    Preconditions.checkArgument(
        expectParallelism != null, "Expect parallelism map can not be null.");
    Preconditions.checkArgument(factor >= 0, "Limit factor is invalid.");

    Map<String, Tuple2<Integer, Integer>> limitResult = new HashMap<>(expectParallelism.size());
    expectParallelism.forEach(
        (k, v) -> {
          int max = (int) (v + v * factor);
          int min = (int) (v - v * factor);
          if (min <= 0) {
            min = 1;
          }
          Tuple2<Integer, Integer> limit = new Tuple2<>(min, max);
          limitResult.put(k, limit);
        });

    return limitResult;
  }

  /**
   * Generate parallelism map for independent operators.
   *
   * @param independentVertices independent vertices list
   * @return parallelism map {actor role type, parallelism}
   */
  public static Map<String, Integer> genParallelismMapForIndependentOperators(
      List<IndependentExecutionVertex> independentVertices) {
    if (independentVertices == null) {
      return Collections.emptyMap();
    }

    Map<String, Integer> resultMap = new HashMap();
    independentVertices.forEach(
        actor -> {
          String type = actor.getActorName();
          resultMap.compute(
              type,
              (k, v) -> {
                if (v == null) {
                  return 1;
                }
                return v + 1;
              });
        });

    return resultMap;
  }

  /**
   * For DAG operator, the operator name is: number-name e.g. 1-SourceOperator
   *
   * <p>For independent operator, the operator name is: name e.g. PARAMETER_SERVER
   *
   * @param operatorName the target operator name
   * @return true if the operator name represent DAG operator
   */
  public static boolean isDagOperator(String operatorName) {
    if (operatorName.contains(OP_NAME_DELIMITER)
        && StringUtils.isNumeric(operatorName.split(OP_NAME_DELIMITER)[0])) {
      return true;
    }
    return false;
  }

  /**
   * Group a list of ExecutionVertex(including IndependentExecutionVertex) to a [JobVertexName, Set
   * of ExeVertex Index] map
   *
   * @param executionVertices ExecutionVertices
   * @param independentVertices IndependentVertices
   * @return A map of group: [JobVertexName, Set of ExeVertex global id]
   */
  public static Map<String, Set<Integer>> groupExecutionVertexByOpName(
      List<ExecutionVertex> executionVertices,
      List<IndependentExecutionVertex> independentVertices) {

    // group by jobVertex name (or ActorRoleType) because this is needed by RescalingInfo
    Map<String, Set<Integer>> vertexMap = new HashMap<>();
    if (!(executionVertices == null || executionVertices.isEmpty())) {
      for (ExecutionVertex executionVertex : executionVertices) {
        String opName = executionVertex.getExecutionJobVertexName();
        LOG.info(
            "opName: {}, map contains? {}, map value: {}",
            opName,
            vertexMap.containsKey(opName),
            vertexMap.get(opName));
        if (!vertexMap.containsKey(opName)) {
          // can't put an ImmutableSet.of since it returns a fixed-sized set that can't be added by
          // new items
          vertexMap.put(opName, new HashSet<>());
        }
        vertexMap.get(opName).add(executionVertex.getExecutionVertexId());
      }
    }
    /* TODO(paer): group independent vertices
    if (!(independentVertices == null || independentVertices.isEmpty())) {
      for (IndependentExecutionVertex vertex : independentVertices) {
        // Desc or name ? RescalingInfo says "ParameterServer" which is the "Desc" rather than the "name"
        String name = vertex.getRoleName().getDesc();
        if (vertexMap.containsKey(name)){
          vertexMap.get(name).add(vertex.getIndex());
        } else {
          vertexMap.put(name, ImmutableSet.of(vertex.getIndex()));
        }
      }
    }
    */

    return vertexMap;
  }
}
