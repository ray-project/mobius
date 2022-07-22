package io.ray.streaming.runtime.master.event;

/** Description message for event. */
public enum EventMessage {

  /** For user to add details. */
  DETAIL_INFO_KEY(" Detail: "),

  /** Common success and failure. */
  SUCCESS("success"),
  FAIL("fail"),

  /** For resource check. */
  RESOURCE_CHECK_AND_UPDATE_SUCCESS("Succeed in resource check."),
  RESOURCE_CHECK_AND_UPDATE_FAIL("Failed in resource check."),

  /** For worker assignment. */
  ASSIGN_WORKER_SUCCESS("Succeed in worker assignment."),
  ASSIGN_WORKER_FAIL("Failed in worker assignment."),

  /** For worker creation. */
  CREATE_WORKER_SUCCESS("Succeed in worker creation."),
  CREATE_WORKER_FAIL("Failed in worker creation."),

  /** For worker initiation. */
  INIT_WORKER_SUCCESS("Succeed in worker initiation."),
  INIT_WORKER_FAIL("Failed in worker initiation."),

  /** For master initiation. */
  INIT_MASTER_SUCCESS("Succeed in master initiation."),
  INIT_MASTER_FAIL("Failed in master initiation."),

  /** For worker initiation and starting. */
  START_WORKER_SUCCESS("Succeed in worker starting."),
  START_WORKER_FAIL("Failed in worker starting."),

  /** For job submission. */
  SUBMIT_JOB_SUCCESS("Succeed in job submission."),
  SUBMIT_JOB_FAIL("Failed in job submission."),

  /** For save checkpoint. */
  SAVE_CHECKPOINT_SUCCESS("Succeed in saving checkpoint."),
  SAVE_CHECKPOINT_FAIL("Failed in saving checkpoint."),

  /** For buffer request. */
  MEMORY_REQUEST_SUCCESS("Succeed in buffer request."),
  MEMORY_REQUEST_FAIL("Failed in buffer request."),

  /** For external optimization. */
  ACTOR_GROUP_RESCALE_REQUEST("Request to rescale actor group."),
  CHECK_FAILED("Failed checking params."),

  /** For rescaling failover. */
  SOFT_RECOVER("Soft recover for rescaling failover."),
  SUBDAG_REBOOT("Reboot subdag to apply the new graph for rescaling failover."),
  DAG_REBOOT("Reboot dag to apply the new graph for rescaling failover."),
  SUBDAG_ROLLBACK("Rollback subdag to recover from rescaling failover."),
  PS_CREATION_FAILED("Failed to create new ps actors when executing rescaling.");

  private String desc;

  EventMessage(String desc) {
    this.desc = desc;
  }

  public String getDesc() {
    return desc;
  }

  public String getDescWithDetail(String detail) {
    return desc + DETAIL_INFO_KEY + detail;
  }
}
