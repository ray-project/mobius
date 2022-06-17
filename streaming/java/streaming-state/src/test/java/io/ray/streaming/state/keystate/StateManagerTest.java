package io.ray.streaming.state.keystate;

import io.ray.streaming.common.metric.local.LocalMetricGroup;
import io.ray.streaming.state.backend.StateBackendType;
import io.ray.streaming.state.config.StateConfig;
import io.ray.streaming.state.keystate.desc.KeyValueStateDescriptor;
import io.ray.streaming.state.keystate.state.KeyValueState;
import io.ray.streaming.state.util.IOUtils;
import java.io.File;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class StateManagerTest {

  private static final Logger LOG = LoggerFactory.getLogger(StateManagerTest.class);

  private final String jobName = "StateManagerTest";
  private String localStoreDir = "/tmp/RayState/rdb/";

  @BeforeMethod
  public void setUp(Method method) throws Exception{
    LOG.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>> Test case: {}.{} began. >>>>>>>>>>>>>>>>>>>>>>>>>>>>",
        method.getDeclaringClass(), method.getName());
    LOG.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>> Test case: {}.{} began. >>>>>>>>>>>>>>>>>>>>>>>>>>>>",
        method.getDeclaringClass(), method.getName());

    File localStoreFile = new File(localStoreDir + method.getName());
    Files.createDirectories(localStoreFile.toPath());
  }

  @AfterMethod
  public void tearDown(Method method) {
    File localStoreFile = new File(localStoreDir + method.getName());
    IOUtils.forceDeleteFile(localStoreFile);
    LOG.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>> Test case: {}.{} end. >>>>>>>>>>>>>>>>>>>>>>>>>>>>",
        method.getDeclaringClass(), method.getName());
  }

  @Test
  public void testStateManagerOfMemoryStore(Method method) throws Exception{
    Map<String, String> config = new HashMap<>();
    config.put(StateConfig.BACKEND_TYPE, StateBackendType.MEMORY.name());
    StateManager memoryStateManager = new StateManager(jobName, method.getName(), config, new LocalMetricGroup());
    KeyValueState<String, Integer> keyValueState =
        memoryStateManager.getKeyValueState(
            KeyValueStateDescriptor.build("testMemory", String.class, Integer.class));

    keyValueState.put("t1", 1);
    Assert.assertEquals((int)keyValueState.get("t1"), 1);

    keyValueState.put("t1", keyValueState.get("t1") + 1);
    Assert.assertEquals((int)keyValueState.get("t1"), 2);
    memoryStateManager.close();
  }
}
