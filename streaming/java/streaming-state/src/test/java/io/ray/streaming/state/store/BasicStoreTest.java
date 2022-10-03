package io.ray.streaming.state.store;

import com.google.common.collect.ImmutableMap;
import io.ray.streaming.state.api.state.MapState;
import io.ray.streaming.state.api.state.ValueState;
import io.ray.streaming.state.manager.StateManager;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;

public abstract class BasicStoreTest {

  protected Map<String, String> config = new HashMap<>();
  protected StateManager stateManager;

  @AfterMethod
  public void end(Method method) {
    if (stateManager != null) {
      stateManager.close();
    }
  }

  protected void testValueBasic(ValueState<String> valueState) throws Exception {
    Assert.assertTrue(StringUtils.isEmpty(valueState.value()));
    valueState.update("0");
    Assert.assertFalse(StringUtils.isEmpty(valueState.value()));
    Assert.assertEquals(valueState.value(), "0");
    valueState.update("1");
    Assert.assertEquals(valueState.value(), "1");
  }

  protected void testKeyValueBasic(MapState<Integer, String> keyValueState) throws Exception {
    keyValueState.put(0, "00");
    Assert.assertEquals(keyValueState.get(0), "00");
    keyValueState.put(0, "01");
    Assert.assertEquals(keyValueState.get(0), "01");
    keyValueState.put(1, "10");
    Assert.assertEquals(keyValueState.get(1), "10");
    Assert.assertTrue(keyValueState.contains(1));
    Assert.assertFalse(keyValueState.contains(2));

    Map<Integer, String> testMap = ImmutableMap.of(0, "02", 1, "12", 2, "22");
    keyValueState.putAll(testMap);
    Assert.assertEquals(keyValueState.get(1), "12");
    Assert.assertTrue(keyValueState.contains(2));
  }
}
