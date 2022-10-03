package io.ray.streaming.runtime.master.scheduler.strategy;

import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Do schedule without placement group. */
public class RandomStrategy extends PlacementGroupAssignBaseStrategy {

  public static final Logger LOG = LoggerFactory.getLogger(RandomStrategy.class);

  @Override
  public void assignForScheduling(ExecutionGraph executionGraph) {
    LOG.info("Skip assignment for random strategy.");
  }

  @Override
  public void assignForDynamicScheduling(ExecutionGraph changedExecutionGraph) {
    LOG.info("Skip dynamic assignment for random strategy.");
  }

  @Override
  public String getName() {
    return null;
  }
}
