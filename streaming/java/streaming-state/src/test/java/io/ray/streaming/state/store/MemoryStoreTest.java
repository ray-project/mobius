package io.ray.streaming.state.store;

import io.ray.streaming.common.metric.local.LocalMetricGroup;
import io.ray.streaming.state.api.desc.MapStateDescriptor;
import io.ray.streaming.state.api.desc.ValueStateDescriptor;
import io.ray.streaming.state.api.state.MapState;
import io.ray.streaming.state.api.state.ValueState;
import io.ray.streaming.state.store.backend.StateBackendType;
import io.ray.streaming.state.config.StateConfig;
import io.ray.streaming.state.manager.StateManager;
import java.lang.reflect.Method;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MemoryStoreTest extends BasicStoreTest {

  @BeforeMethod
  public void prepare(Method method) {
    config.put(StateConfig.BACKEND_TYPE, StateBackendType.MEMORY.name());
    config.put("streaming.operator.state.backend.type", "MEMORY");
  }

  @Test
  public void testNonKeyedValueBasic(Method method) throws Exception {
    stateManager = new StateManager(
        "test-nonKeyed-key-value", method.getName(), config, new LocalMetricGroup());
    ValueState<String> valueState = stateManager.getNonKeyedValueState(
        ValueStateDescriptor.build("testNonKeyedValue", String.class));

    super.testValueBasic(valueState);
  }

  @Test
  public void testNonKeyedKeyValueBasic(Method method) throws Exception {
    stateManager = new StateManager(
        "test-nonKeyed-key-value", method.getName(), config, new LocalMetricGroup());
    MapState<Integer, String> keyValueState = stateManager.getNonKeyedMapState(
        MapStateDescriptor.build("testNonKeyedKeyValue", Integer.class, String.class));

    super.testKeyValueBasic(keyValueState);
  }
}
