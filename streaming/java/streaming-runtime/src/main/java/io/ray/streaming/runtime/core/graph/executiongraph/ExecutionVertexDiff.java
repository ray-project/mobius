package io.ray.streaming.runtime.core.graph.executiongraph;

import java.util.ArrayList;
import java.util.List;

public class ExecutionVertexDiff {

  private List<ExecutionVertex> aliveExecutionVertices;
  private List<ExecutionVertex> moribundExecutionVertices;

  public ExecutionVertexDiff(
      List<ExecutionVertex> aliveExecutionVertices,
      List<ExecutionVertex> moribundExecutionVertices) {
    this.aliveExecutionVertices = aliveExecutionVertices;
    this.moribundExecutionVertices = moribundExecutionVertices;
  }

  public List<ExecutionVertex> getAllExecutionVertices() {
    List<ExecutionVertex> allExecutionVertices = new ArrayList<>(aliveExecutionVertices);
    allExecutionVertices.addAll(moribundExecutionVertices);
    return allExecutionVertices;
  }

  public List<ExecutionVertex> getAliveExecutionVertices() {
    return aliveExecutionVertices;
  }

  public List<ExecutionVertex> getMoribundExecutionVertices() {
    return moribundExecutionVertices;
  }
}
