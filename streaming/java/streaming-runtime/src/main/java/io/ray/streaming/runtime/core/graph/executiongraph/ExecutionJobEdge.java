package io.ray.streaming.runtime.core.graph.executiongraph;

import com.google.common.base.MoreObjects;
import io.ray.streaming.api.partition.Partition;
import io.ray.streaming.jobgraph.JobEdge;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/** An edge that connects two execution job vertices. */
public class ExecutionJobEdge implements Serializable {

  /** The source(upstream) execution job vertex. */
  private final ExecutionJobVertex sourceExecutionJobVertex;

  /** The target(downstream) execution job vertex. */
  private final ExecutionJobVertex targetExecutionJobVertex;

  /** The partition of the execution job edge. */
  private final Partition partition;

  /** An unique id for execution job edge. */
  private final String executionJobEdgeIndex;

  /** An unique id for current execution job vertices' upstream and downstream. */
  private List<ExecutionJobEdge> inputs = new ArrayList<>();

  private List<ExecutionJobEdge> outputs = new ArrayList<>();

  /** All execution edges of current execution job edge. */
  private List<ExecutionEdge> executionEdges;

  public ExecutionJobEdge(
      ExecutionJobVertex sourceExecutionJobVertex,
      ExecutionJobVertex targetExecutionJobVertex,
      JobEdge jobEdge) {
    this.sourceExecutionJobVertex = sourceExecutionJobVertex;
    this.targetExecutionJobVertex = targetExecutionJobVertex;
    this.partition = jobEdge.getPartition();
    this.executionJobEdgeIndex = generateExecutionJobEdgeIndex();
  }

  private String generateExecutionJobEdgeIndex() {
    return sourceExecutionJobVertex.getExecutionJobVertexId()
        + "—"
        + targetExecutionJobVertex.getExecutionJobVertexId();
  }

  public ExecutionJobVertex getSource() {
    return sourceExecutionJobVertex;
  }

  public ExecutionJobVertex getTarget() {
    return targetExecutionJobVertex;
  }

  public Partition getPartition() {
    return partition;
  }

  public List<ExecutionEdge> getExecutionEdges() {
    return executionEdges;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("source", sourceExecutionJobVertex)
        .add("target", targetExecutionJobVertex)
        .add("partition", partition)
        .add("index", executionJobEdgeIndex)
        .toString();
  }
}
