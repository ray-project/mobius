package io.ray.streaming.runtime.config.master;

import io.ray.streaming.runtime.config.Config;

/** Configuration for job scheduler. */
public interface SchedulerConfig extends Config {

  String STRATEGY_TYPE = "streaming.scheduler.strategy.type";
  String MAX_PARALLELISM = "streaming.job.max.parallelism";
  String JOB_SUBMISSION_WORKER_WAIT_TIMEOUT_MS =
      "streaming.scheduler.job-submission.worker.timeout.ms";
  String SCALING_NUM_PER_CHECKPOINT = "streaming.scheduler.scaling.num.per.checkpoint";
  String RESCALING_WORKER_WAIT_TIMEOUT_MS = "streaming.scheduler.rescaling.worker.timeout.ms";
  String RESCALING_PLACEMENTGROUP_WAIT_TIMEOUT_S =
      "streaming.scheduler.rescaling.placementgroup.timeout.sec";
  String RESCALING_MANDATORY_SUCCESS_ENABLE =
      "streaming.scheduler.rescaling.mandatory-success.enable";
  String RESCALING_FO_RESUBMIT_TYPE = "streaming.scheduler.rescaling.failover-resubmit.type";
  String ENABLE_DYNAMIC_DIVISION = "streaming.scheduler.dynamic.division.enable";
  String PARAMETER_SERVER_RESCALING_TIMEOUT = "streaming.scheduler.ps.rescaling.timeout.sec";
  String EXTERNAL_INTERACTION_ENABLE = "streaming.external.interaction.enable";

  String RESCALING_PLACEMENTGROUP_WAIT_TIMEOUT_S_DEFAULT = "5";

  @DefaultValue(value = "pipeline_first")
  @Key(value = STRATEGY_TYPE)
  String placementGroupAssignStrategy();

  @DefaultValue(value = "1024")
  @Key(value = MAX_PARALLELISM)
  int maxParallelism();

  /**
   * 10 minutes by default
   *
   * @return value in ms
   */
  @Key(JOB_SUBMISSION_WORKER_WAIT_TIMEOUT_MS)
  @DefaultValue(value = "600000")
  int jobSubmissionWorkerWaitTimeoutMs();

  /**
   * 5 minutes by default
   *
   * @return value in ms
   */
  @Key(RESCALING_WORKER_WAIT_TIMEOUT_MS)
  @DefaultValue(value = "300000")
  int rescalingWorkerWaitTimeoutMs();

  /**
   * 5 seconds by default. Used in {@link ExecutionGroup#buildPlacementGroup(java.util.Map)}
   *
   * @return value in second
   */
  @Key(RESCALING_PLACEMENTGROUP_WAIT_TIMEOUT_S)
  @DefaultValue(value = RESCALING_PLACEMENTGROUP_WAIT_TIMEOUT_S_DEFAULT)
  int rescalingPlacementGroupWaitTimeoutMs();

  /**
   * Whether need to resubmit to finish rescaling when rescaling meet failover.
   *
   * @return true: resubmit when rescaling meet failover
   */
  @Key(RESCALING_MANDATORY_SUCCESS_ENABLE)
  @DefaultValue("true")
  boolean rescalingFailoverResubmitEnable();

  /**
   * Resubmit the whole dag or just the subdag.
   *
   * @return true: DAG or SUBDAG
   */
  @Key(RESCALING_FO_RESUBMIT_TYPE)
  @DefaultValue("DAG")
  String rescalingFailoverResubmitType();

  @Key(SCALING_NUM_PER_CHECKPOINT)
  @DefaultValue("1")
  int scalingNumPerCheckpoint();

  @Key(ENABLE_DYNAMIC_DIVISION)
  @DefaultValue("false")
  boolean enableDynamicDivision();

  @Key(PARAMETER_SERVER_RESCALING_TIMEOUT)
  @DefaultValue("3600")
  int psRescalingTimeout();

  /**
   * Enable interaction with external system by system actor.
   *
   * @return the result
   */
  @DefaultValue(value = "true")
  @Key(value = EXTERNAL_INTERACTION_ENABLE)
  boolean externalInteractionEnable();
}
