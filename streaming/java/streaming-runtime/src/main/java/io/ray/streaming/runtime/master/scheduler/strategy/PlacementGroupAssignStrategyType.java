package io.ray.streaming.runtime.master.scheduler.strategy;

public enum PlacementGroupAssignStrategyType {

  /** RANDOM(without placement group) */
  RANDOM("random", -1),

  /** PIPELINE_FIRST */
  PIPELINE_FIRST("pipeline_first", 0);

  private String name;
  private int index;

  PlacementGroupAssignStrategyType(String name, int index) {
    this.name = name;
    this.index = index;
  }

  public String getName() {
    return name;
  }
}
