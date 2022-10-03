package io.ray.streaming.runtime.master.scheduler;

import com.google.common.base.MoreObjects;

/** Actor Role. */
public enum ActorRoleType {

  /** Unknown. */
  UNKNOWN("Unknown", -1),

  /** Master actor. */
  JOB_MASTER("JobMaster", 1),

  /** Worker actor. */
  JOB_WORKER("JobWorker", 2),

  /** Broker actor for queryable state. */
  BROKER("Broker", 3),

  /** Shuffle manager actor for queryable state. */
  SHUFFLE_MANAGER("ShuffleManager", 4),

  /** Dsc server for streaming call. */
  DSC_SERVER("DSCServer", 5),

  /** Parameter server for training worker. */
  PARAMETER_SERVER("ParameterServer", 6),

  /** Evaluator for training worker. */
  EVALUATOR("Evaluator", 7),

  /** To optimize training worker by interacting with easy-dl. */
  OPTIMIZER("Optimizer", 8),

  /** Normal Independent operator */
  INDEPENDENT_OPERATOR("IndependentOperator", 9);

  private String desc;
  private int index;

  ActorRoleType(String desc, int index) {
    this.desc = desc;
    this.index = index;
  }

  public String getDesc() {
    return desc;
  }

  public static ActorRoleType valueOfNameOrDesc(String type) {
    try {
      // get value by name
      return ActorRoleType.valueOf(type);
    } catch (IllegalArgumentException e) {
      // get value by desc
      for (ActorRoleType actorRoleType : ActorRoleType.class.getEnumConstants()) {
        if (actorRoleType.getDesc().equals(type)) {
          return actorRoleType;
        }
      }
    }
    return ActorRoleType.UNKNOWN;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("Desc", desc).toString();
  }
}
