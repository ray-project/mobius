package io.ray.streaming.runtime.master.scheduler.strategy;

/**
 * Base implementation of {@link PlacementGroupAssignStrategy}.
 */
public abstract class PlacementGroupAssignBaseStrategy implements PlacementGroupAssignStrategy {

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }

}
