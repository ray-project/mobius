package io.ray.streaming.runtime.core.graph.executiongraph;

import com.google.common.base.MoreObjects;
import io.ray.streaming.api.partition.Partition;
import java.io.Serializable;

/** An edge that connects two execution vertices. */
public class ExecutionEdge implements Serializable {

  /** The source(upstream) execution vertex. */
  private ExecutionVertex sourceExecutionVertex;

  /** The target(downstream) execution vertex. */
  private ExecutionVertex targetExecutionVertex;

  /** The partition of current execution edge's execution job edge. */
  private final Partition partition;

  /** An unique id for execution edge. */
  private final String executionEdgeIndex;

  public ExecutionEdge(
      ExecutionVertex sourceExecutionVertex,
      ExecutionVertex targetExecutionVertex,
      ExecutionJobEdge executionJobEdge) {
    this.sourceExecutionVertex = sourceExecutionVertex;
    this.targetExecutionVertex = targetExecutionVertex;
    this.partition = executionJobEdge.getPartition();
    this.executionEdgeIndex = generateExecutionEdgeIndex();
  }

  private String generateExecutionEdgeIndex() {
    return sourceExecutionVertex.getExecutionVertexId()
        + "—"
        + targetExecutionVertex.getExecutionVertexId();
  }

  public ExecutionVertex getSource() {
    return sourceExecutionVertex;
  }

  public ExecutionVertex getTarget() {
    return targetExecutionVertex;
  }

  public void setSource(ExecutionVertex source) {
    this.sourceExecutionVertex = source;
  }

  public void setTarget(ExecutionVertex target) {
    this.targetExecutionVertex = target;
  }

  public String getTargetExecutionJobVertexName() {
    return getTarget().getExecutionJobVertexName();
  }

  public int getSourceVertexId() {
    return sourceExecutionVertex.getExecutionVertexId();
  }

  public int getTargetVertexId() {
    return targetExecutionVertex.getExecutionVertexId();
  }

  public String getExecutionEdgeId() {
    return getSourceVertexId() + "_" + getTargetVertexId();
  }

  public String getExecutionEdgeName() {
    return getSource().getExecutionVertexName() + "_" + getTarget().getExecutionVertexName();
  }

  public Partition getPartition() {
    return partition;
  }

  public String getExecutionEdgeIndex() {
    return executionEdgeIndex;
  }

  public boolean isAlive() {
    return !getSource().isToDelete() && !getTarget().isToDelete();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("source", sourceExecutionVertex)
        .add("target", targetExecutionVertex)
        .add("partition", partition)
        .add("index", executionEdgeIndex)
        .toString();
  }
}
