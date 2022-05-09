package io.ray.streaming.runtime.master.scheduler.strategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PlacementGroupAssignStrategyFactory {

  private static final Logger LOG = LoggerFactory.getLogger(PlacementGroupAssignStrategyFactory.class);

  public static PlacementGroupAssignStrategy getStrategy(final PlacementGroupAssignStrategyType type) {
    PlacementGroupAssignStrategy strategy;
    LOG.info("Placement group assign strategy is: {}.", type);

    switch (type) {
      case PIPELINE_FIRST:
        // TODO
      default:
        strategy = new RandomStrategy();
    }
    return strategy;
  }

  public static PlacementGroupAssignStrategyType getType(PlacementGroupAssignStrategy strategy) {
      return PlacementGroupAssignStrategyType.RANDOM;
  }
}
