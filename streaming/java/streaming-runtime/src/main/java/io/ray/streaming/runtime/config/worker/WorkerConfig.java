package io.ray.streaming.runtime.config.worker;

import io.ray.streaming.common.config.Config;

public interface WorkerConfig extends Config {

  String WORKER_ID_INTERNAL = WorkerConfig.PY_WORKER_ID;
  String WORKER_OP_NAME_INTERNAL = "opName";
  String WORKER_PARALLELISM_INTERNAL = "worker_parallelism";
  String WORKER_PARALLELISM_INDEX_INTERNAL = "worker_parallelism_index";
//  String WORKER_NAME_INTERNAL = QueueConfigKeys.STREAMING_WORKER_NAME;
  String WORKER_ACTOR_NAME_INTERNAL = "StreamingWorkerActorName";
//  String OPERATOR_TYPE_INTERNAL = QueueConfigKeys.OPERATOR_TYPE;
//  String OPERATOR_NAME_INTERNAL = QueueConfigKeys.STREAMING_OP_NAME;
  // just use CommonConfig.JOB_NAME
//  String JOB_NAME_INTERNAL = QueueConfigKeys.STREAMING_JOB_NAME;
//  String RELIABILITY_LEVEL_INTERNAL = QueueConfigKeys.RELIABILITY_LEVEL;
//  String INDIVIDUAL_CONFIG = QueueConfigKeys.STREAMING_INDIVIDUAL_CONF;
  String STATE_VERSION = "streaming.state.version";

  /**
   * The following key must be equal with StreamingConstants in python
   * package: python.streaming.runtime.core.constant
   * py: streaming_constants.py
   */
  String PY_CP_MODE = "save_checkpoint_mode";
  String PY_CP_MODE_PY = "save_checkpoint_mode_py";
  String PY_CP_STATE_BACKEND_TYPE = "cp_state_backend_type";
  String PY_CP_MEMORY_BACKEND = "cp_state_backend_memory";
  //pangu
  String PY_CP_PANGU_BACKEND = "cp_state_backend_pangu";
  String PY_CP_PANGU_CLUSTER_NAME = "cp_pangu_cluster_name";
  String PY_CP_PANGU_ROOT_DIR = "cp_pangu_root_dir";
  String PY_CP_PANGU_USER_MYSQL_URL = "cp_pangu_user_mysql_url";
  //dfs
  String PY_CP_DFS_BACKEND = "cp_state_backend_dfs";
  String PY_CP_DFS_CLUSTER_NAME = "cp_dfs_cluster_name";
  String PY_CP_DFS_ROOT_DIR = "cp_dfs_root_dir";
  //  String PY_CP_LOCAL_DISK_ROOT_DIR = "cp_local_disk_root_dir";
  String PY_METRICS_TYPE = "metrics_type";
  String PY_METRICS_URL = "metrics_url";
  String PY_METRICS_USER_NAME = "metrics_user_name";
  String PY_RELIABILITY_LEVEL = "Reliability_Level";
  String PY_QUEUE_TYPE = "queue_type";
  String PY_QUEUE_SIZE = "queue_size";
  String PY_QUEUE_TIMER_INTERVAL = "timerIntervalMs";
  String PY_QUEUE_PULL_TIMEOUT = "queue_pull_timeout";
  String PY_WORKER_ID = "worker_id";

  // This flag controls whether "dynamic rebalance" replaces "forward".
  String ENABLE_DYNAMIC_REBALANCE = "streaming.partition.enable_dr";

  // The maximum number of consecutive "dynamic rebalance" sends.
  String DYNAMIC_REBALANCE_BATCH_SIZE = "streaming.partition.batch_size";

  String UPDATE_CONTEXT_OVERRIDE_ENABLE = "streaming.worker.update-context.override.enable";

  @DefaultValue(value = "default-worker-id")
  @Key(value = WORKER_ID_INTERNAL)
  String workerId();

//  @DefaultValue(value = "default-worker-name")
//  @Key(value = WORKER_NAME_INTERNAL)
//  String workerName();
//
//  @DefaultValue(value = "SOURCE")
//  @Key(value = OPERATOR_TYPE_INTERNAL)
//  String operatorType();
//
//  @DefaultValue(value = "default-operator-name")
//  @Key(value = OPERATOR_NAME_INTERNAL)
//  String operatorName();
//
//  @DefaultValue(value = "")
//  @Key(value = INDIVIDUAL_CONFIG)
//  String individualConfig();

  @DefaultValue(value = "")
  @Key(value = STATE_VERSION)
  String stateVersion();

  @DefaultValue(value = "true")
  @Key(value = ENABLE_DYNAMIC_REBALANCE)
  boolean enableDynamicRebalance();

  @DefaultValue(value = "50")
  @Key(value = DYNAMIC_REBALANCE_BATCH_SIZE)
  int dynamicRebalanceBatchSize();

  /**
   * If set to true, worker will override the new context for the
   * last successful checkpoint's context when update context is finished.
   *
   * @return true: override context when update context
   */
  @DefaultValue(value = "true")
  @Key(value = UPDATE_CONTEXT_OVERRIDE_ENABLE)
  boolean updateContextOverrideEnable();
}
