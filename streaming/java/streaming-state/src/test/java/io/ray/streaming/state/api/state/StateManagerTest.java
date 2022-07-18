package io.ray.streaming.state.api.state;

import io.ray.streaming.common.metric.local.LocalMetricGroup;
import io.ray.streaming.state.api.desc.MapStateDescriptor;
import io.ray.streaming.state.backend.StateBackendType;
import io.ray.streaming.state.config.StateConfig;
import io.ray.streaming.state.manager.StateManager;
import io.ray.streaming.state.util.IOUtils;
import java.io.File;
import java.lang.reflect.Method;
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
    MapState<String, Integer> keyValueState =
        memoryStateManager.getMapState(MapStateDescriptor.build("testMemory", String.class, Integer.class));

    keyValueState.put("t1", 1);
    Assert.assertEquals((int)keyValueState.get("t1"), 1);

    keyValueState.put("t1", keyValueState.get("t1") + 1);
    Assert.assertEquals((int)keyValueState.get("t1"), 2);
    memoryStateManager.close();
  }

  @Test
  public void testGenericDataType(Method method) throws Exception {
    Map<String, String> config = new HashMap<>();
    config.put(StateConfig.BACKEND_TYPE, StateBackendType.MEMORY.name());
//    config.put(HDFSBackendConfig.STATE_BACKEND_HDFS_STORE_TYPE, HDFSStoreType.LOCAL.name());
//    config.put(RocksDBBackendConfig.STATE_BACKEND_ROCKSDB_REMOTE_STORE_ENABLE, "false");
//    config.put(RocksDBBackendConfig.STATE_MAX_SHARD, "1");
//    config.put(RocksDBBackendConfig.STATE_BACKEND_ROCKSDB_STORE_DIR, localStoreDir + method.getName());
    StateManager stateManager = new StateManager(jobName, method.getName(), config, new LocalMetricGroup());

    //test special data type
    MapState<String, Person> specialDataKeyValueState = stateManager.getMapState(
        MapStateDescriptor.build("testSpecialData", String.class, Person.class));
    specialDataKeyValueState.put("zhansan", new Person("zhansan", 27));
    specialDataKeyValueState.put("lisi", new Person("lisi", 30));
    Assert.assertEquals(specialDataKeyValueState.get("zhansan").getUserName(), "zhansan");
    Assert.assertEquals(specialDataKeyValueState.get("lisi").getUserName(), "lisi");
    stateManager.close();
  }

  static class Person {
    private String userName;
    private int age;

    public Person() {}

    public Person(String userName, int age) {
      this.userName = userName;
      this.age = age;
    }

    public String getUserName() {
      return userName;
    }

    public int getAge() {
      return age;
    }
  }
}
