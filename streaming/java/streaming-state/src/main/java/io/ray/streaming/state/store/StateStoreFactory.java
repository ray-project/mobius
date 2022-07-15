package io.ray.streaming.state.store;

import io.ray.streaming.common.metric.MetricGroup;
import io.ray.streaming.state.backend.StateBackendType;
import io.ray.streaming.state.store.memory.MemoryStoreManager;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StateStoreFactory {

  private static final Logger LOG = LoggerFactory.getLogger("StateStoreFactory");

  public static StoreManager build(final StateBackendType backendType,
                                   final String jobName,
                                   final String stateName,
                                   final Map<String, String> config,
                                   final MetricGroup metricGroup) {
    LOG.info("Begin create {} store manager", backendType.name());
    switch (backendType) {
      case MEMORY:
        return new MemoryStoreManager(jobName, stateName, config, metricGroup);
      default:
        throw new UnsupportedOperationException("Current support backend type: [MEMORY, "
            + "]. Not found" + backendType + "type.");
    }
  }
}