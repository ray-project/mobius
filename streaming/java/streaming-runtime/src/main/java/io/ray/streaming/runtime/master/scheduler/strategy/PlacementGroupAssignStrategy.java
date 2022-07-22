package io.ray.streaming.runtime.master.scheduler.strategy;

import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionGraph;

/** Strategy for worker's allocation by using placement group. */
public interface PlacementGroupAssignStrategy {

  /**
   * Generate placement group info by executionGraph.
   *
   * @param executionGraph the physical plan
   */
  void assignForScheduling(ExecutionGraph executionGraph);

  /**
   * Modify placement group info by graph diff.
   *
   * @param changedExecutionGraph the changed physical plan
   */
  void assignForDynamicScheduling(ExecutionGraph changedExecutionGraph);

  /** Get strategy name. */
  String getName();
}
