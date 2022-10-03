package io.ray.streaming.common.config;

public interface ResourceConfig extends Config {

  String MASTER_MEM = "streaming.resource.master.buffer.mb";
  String MASTER_PER_HUNDRED_PARALLELISM_MEM =
      "streaming.resource.master.per-hundred-parallelism.buffer.gb";
  String WORKER_RESOURCE_STRICT_LIMIT_ENABLE = "streaming.resource.worker.strict-limit.enable";
  String WORKER_CPU = "streaming.resource.worker.cpu";
  String WORKER_MEM = "streaming.resource.worker.mem.mb";
  String WORKER_GPU = "streaming.resource.worker.gpu";
  String EXTRA_CPU = "streaming.resource.extra.cpu";
  String EXTRA_MEMORY_MB = "streaming.resource.extra.buffer.mb";
  String WORKER_CPU_LIMIT = "streaming.resource.worker.cpu.limit";
  String WORKER_MEM_LIMIT = "streaming.resource.worker.mem.limit";
  String WORKER_GPU_LIMIT = "streaming.resource.worker.gpu.limit";
  String WORKER_CPU_RECOMMENDATION_FACTOR =
      "streaming.resource.worker.cpu.recommendation.multiple.factor";
  String WORKER_MEM_RECOMMENDATION_FACTOR =
      "streaming.resource.worker.mem.recommendation.multiple.factor";
  String WORKER_GPU_RECOMMENDATION_FACTOR =
      "streaming.resource.worker.gpu.recommendation.multiple.factor";
  String OPERATOR_CUSTOM_RESOURCE = "streaming.resource.operator.custom.resource";
  String ENABLE_OPERATOR_PROPOSED_RESOURCE = "streaming.resource.operator.proposed.resource.enable";
  String OPERATOR_PROPOSED_RESOURCE = "streaming.resource.operator.proposed.resource";

  /* Belows are copied from io.ray.streaming.runtime.config.master.ResourceConfig */
  /** Number of actors per container. */
  String MAX_ACTOR_NUM_PER_CONTAINER = "streaming.container.per.max.actor";
  /** The interval between detecting ray cluster nodes. */
  String CONTAINER_RESOURCE_CHECk_INTERVAL_SECOND = "streaming.resource.check.interval.second";
  /** CPU use by per task. */
  String TASK_RESOURCE_CPU = "streaming.task.resource.cpu";
  /** Memory used by each task */
  String TASK_RESOURCE_MEM = "streaming.task.resource.mem";
  /** Whether to enable CPU limit in resource control. */
  String TASK_RESOURCE_CPU_LIMIT_ENABLE = "streaming.task.resource.cpu.limitation.enable";
  /** Whether to enable buffer limit in resource control. */
  String TASK_RESOURCE_MEM_LIMIT_ENABLE = "streaming.task.resource.mem.limitation.enable";
  /* End of io.ray.streaming.runtime.config.master.ResourceConfig */

  long JOB_MASTER_MAX_MEMORY_MB = 10000;

  /**
   * Memory for job master.
   *
   * @return job master's buffer in mb
   */
  @DefaultValue(value = "3000")
  @Key(value = MASTER_MEM)
  int masterMemoryMb();

  /**
   * Gb buffer for job master per hundred parallelism. e.g. total parallelism: 400 -> buffer:
   * 400/100 * 1 = 4gb
   *
   * @return gb buffer
   */
  @DefaultValue(value = "1")
  @Key(value = MASTER_PER_HUNDRED_PARALLELISM_MEM)
  int masterMemoryGbPerHundredParallelism();

  /**
   * If this value is false, scheduler will not care whether the worker has cpu or mem required.
   *
   * @return true or false
   */
  @DefaultValue(value = "true")
  @Key(value = WORKER_RESOURCE_STRICT_LIMIT_ENABLE)
  boolean isWorkerResourceStrictLimit();

  /**
   * Default cpu required for each worker. Unit: number. Warning: do not use float value(only
   * integer is supported) in cluster mode
   *
   * @return cpu number
   */
  @DefaultValue(value = "1")
  @Key(value = WORKER_CPU)
  double workerCpuRequired();

  /**
   * Default buffer required for each worker unless jvm-opts is specified. Unit: mb. Warning: do not
   * use float value(only integer is supported) in cluster mode
   *
   * @return buffer size
   */
  @DefaultValue(value = "1600")
  @Key(value = WORKER_MEM)
  double workerMemMbRequired();

  /**
   * Default gpu required for each worker. Unit: number. Warning: do not use float value(only
   * integer is supported) in cluster mode
   *
   * @return gpu number
   */
  @DefaultValue(value = "0")
  @Key(value = WORKER_GPU)
  double workerGpuRequired();

  /**
   * Extra cpu for job(including in total cpu number).
   *
   * @return cpu number
   */
  @DefaultValue(value = "0")
  @Key(value = EXTRA_CPU)
  double extraCpuNum();

  /**
   * Extra buffer for job(including in total buffer).
   *
   * @return buffer in mb
   */
  @DefaultValue(value = "4000")
  @Key(value = EXTRA_MEMORY_MB)
  long extraMemoryMb();

  /**
   * User defined operator resource in job config. Must be json style, format: {opId, {resources'
   * json}} or {opName, {resources' json}}
   *
   * <p>e.g. {"10":{"CPU": "2"},"12":{"CPU": "3", "MEM": "4000"}} or {"10-MapOperator":{"CPU":
   * "2"},"12-SourceOperator":{"CPU": "3", "MEM": "4000"}} or {"10":{"CPU":
   * "2"},"12-SourceOperator":{"CPU": "3", "MEM": "4000"}}
   *
   * @return operator resource definition in json style
   */
  @DefaultValue(value = "{}")
  @Key(value = OPERATOR_CUSTOM_RESOURCE)
  String operatorCustomResource();

  /**
   * Whether to enable the default proposed operator resources.
   *
   * @return true or false
   */
  @DefaultValue(value = "true")
  @Key(value = ENABLE_OPERATOR_PROPOSED_RESOURCE)
  boolean enableProposedResource();

  /**
   * Default operator resource suggestion. Must be json style, format: {operator key name,
   * {resources' json}} or {operator key name, {resources' json}}
   *
   * <p>e.g. {"Source:{"CPU": "1", "MEM": "1200"},"Filter":{"CPU": "1", "MEM": "1200"},
   * "Map":{"CPU": "1", "MEM": "1200"},"Join":{"CPU": "1", "MEM": "2000"}}
   *
   * @return operator resource definition in json style
   */
  @DefaultValue(
      value =
          "{\"Source\":{\"MEM\": \"1200\"},"
              + "\"Join\":{\"CPU\": \"1\", \"MEM\": \"4000\"},"
              + "\"Window\":{\"CPU\": \"1\", \"MEM\": \"4000\"}}")
  @Key(value = OPERATOR_PROPOSED_RESOURCE)
  String operatorProposedResource();

  /**
   * Limit of the cpu resource for all kind of worker. Unit: number. format: minimum,maximum
   *
   * @return cpu limit
   */
  @DefaultValue(value = "0.1,8")
  @Separator(",")
  @Key(value = WORKER_CPU_LIMIT)
  float[] workerCpuLimit();

  /**
   * Limit of the buffer resource for all kind of worker. Unit: mb. format: minimum,maximum
   *
   * @return buffer limit
   */
  @DefaultValue(value = "1000,128000")
  @Separator(",")
  @Key(value = WORKER_MEM_LIMIT)
  float[] workerMemLimit();

  /**
   * Limit of the gpu resource for all kind of worker. Unit: number. format: minimum,maximum
   *
   * @return gpu limit
   */
  @DefaultValue(value = "0.1,4")
  @Separator(",")
  @Key(value = WORKER_GPU_LIMIT)
  float[] workerGpuLimit();

  /**
   * Recommendation multiple factor for Cpu resource. The recommendation vale is valid if the
   * recommendation vale >= the current value * factor.
   *
   * @return cpu factor
   */
  @DefaultValue(value = "2")
  @Key(value = WORKER_CPU_RECOMMENDATION_FACTOR)
  float workerCpuRecommendationFactor();

  /**
   * Recommendation multiple factor for Memory resource. The recommendation vale is valid if the
   * recommendation vale >= the current value * factor.
   *
   * @return buffer factor
   */
  @DefaultValue(value = "1.3")
  @Key(value = WORKER_MEM_RECOMMENDATION_FACTOR)
  float workerMemRecommendationFactor();

  /**
   * Recommendation multiple factor for Gpu resource. The recommendation vale is valid if the
   * recommendation vale >= the current value * factor.
   *
   * @return gpu factor
   */
  @DefaultValue(value = "2")
  @Key(value = WORKER_GPU_RECOMMENDATION_FACTOR)
  float workerGpuRecommendationFactor();

  /* Belows are copied from io.ray.streaming.runtime.config.master.ResourceConfig */
  /** Number of cpu per task. */
  @DefaultValue(value = "1.0")
  @Key(value = TASK_RESOURCE_CPU)
  double taskCpuResource();

  /** Memory size used by each task. */
  @DefaultValue(value = "2.0")
  @Key(value = TASK_RESOURCE_MEM)
  double taskMemResource();

  /** Whether to enable CPU limit in resource control. */
  @DefaultValue(value = "false")
  @Key(value = TASK_RESOURCE_CPU_LIMIT_ENABLE)
  boolean isTaskCpuResourceLimit();

  /** Whether to enable buffer limit in resource control. */
  @DefaultValue(value = "false")
  @Key(value = TASK_RESOURCE_MEM_LIMIT_ENABLE)
  boolean isTaskMemResourceLimit();

  /** Number of actors per container. */
  @DefaultValue(value = "500")
  @Key(MAX_ACTOR_NUM_PER_CONTAINER)
  int actorNumPerContainer();

  /** The interval between detecting ray cluster nodes. */
  @DefaultValue(value = "1")
  @Key(value = CONTAINER_RESOURCE_CHECk_INTERVAL_SECOND)
  long resourceCheckIntervalSecond();
  /* End of io.ray.streaming.runtime.config.master.ResourceConfig */
}
