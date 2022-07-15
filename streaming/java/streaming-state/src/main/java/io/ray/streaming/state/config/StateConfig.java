package io.ray.streaming.state.config;

import io.ray.streaming.state.backend.StateBackendType;
import org.aeonbits.owner.Config;

public interface StateConfig extends Config {

  String BACKEND_TYPE = "state.backend.type";

  String BACKEND_TTL = "state.backend.ttl";

  String STATE_CHECKPOINT_RETAINED_NUM = "state.checkpoint.retained.num";

  String STATE_CHECKPOINT_INCREMENT_ENABLE = "state.checkpoint.increment.enable";

  String STATE_MAX_SHARD = "state.max.shard";

  String STATE_MAX_SHARD_DEFAULT_VALUE_STR = "65536";

  int STATE_MAX_SHARD_DEFAULT_VALUE = Integer.parseInt(STATE_MAX_SHARD_DEFAULT_VALUE_STR);

  String STATE_ENABLE_DEBUG_LOG = "state.enable.debug.log";

  String STATE_SERIALIZER_TYPE = "state.serializer.type";

  public static String ASYNC_CHEKCPOINT_MODE = "state.checkpoint.async.mode";

  String STATE_LOCAL_STORE_USE_JOB_DIR = "state.local.store.use.job.dir.enable";

  String STATE_TABLE_NAME = "state.table.name";

  @DefaultValue("MEMORY")
  @Key(value = BACKEND_TYPE)
  StateBackendType stateBackendType();

  @DefaultValue(value = "10800")
  @Key(BACKEND_TTL)
  int backendTTL();

  @DefaultValue("2")
  @Key(value = STATE_CHECKPOINT_RETAINED_NUM)
  int retainedCheckpointNum();

  @DefaultValue("true")
  @Key(value = STATE_CHECKPOINT_INCREMENT_ENABLE)
  boolean isIncrementCheckpoint();

  @DefaultValue(STATE_MAX_SHARD_DEFAULT_VALUE_STR)
  @Key(value = STATE_MAX_SHARD)
  int maxShard();

  @DefaultValue("false")
  @Key(value = STATE_ENABLE_DEBUG_LOG)
  boolean enableDebugLog();

  @DefaultValue("false")
  @Key(value = STATE_LOCAL_STORE_USE_JOB_DIR)
  boolean enableJobStoreDir();

  @DefaultValue("table")
  @Key(value = STATE_TABLE_NAME)
  String getStateTableName();
}
