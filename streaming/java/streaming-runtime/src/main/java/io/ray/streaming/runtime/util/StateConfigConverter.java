package io.ray.streaming.runtime.util;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;

/**
 * Convert realtime state config to ray state config.
 */
public class StateConfigConverter {

  public static final String CHECKPOINT_STATE_CONFIG_PREFIX = "streaming.checkpoint";
  public static final String OPERATOR_STATE_CONFIG_PREFIX = "streaming.operator";

  public static Map<String, String> convertCheckpointStateConfig(Map<String, String> config) {
    Preconditions.checkNotNull(config, "Config must not null!");
    Map<String, String> stateConfig = new HashMap<>(4);
    for (Map.Entry<String, String> entry : config.entrySet()) {
      if (entry.getKey().startsWith(CHECKPOINT_STATE_CONFIG_PREFIX)) {
        stateConfig.put(entry.getKey().substring(CHECKPOINT_STATE_CONFIG_PREFIX.length() + 1), entry.getValue());
      }
    }
    return stateConfig;
  }

  public static Map<String, String> convertOperatorStateConfig(Map<String, String> config) {
    Preconditions.checkNotNull(config, "Config must not null!");
    Map<String, String> stateConfig = new HashMap<>(4);
    for (Map.Entry<String, String> entry : config.entrySet()) {
      if (entry.getKey().startsWith(OPERATOR_STATE_CONFIG_PREFIX)) {
        stateConfig.put(entry.getKey().substring(OPERATOR_STATE_CONFIG_PREFIX.length() + 1), entry.getValue());
      }
    }
    return stateConfig;
  }
}
