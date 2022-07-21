package io.ray.streaming.runtime.master;

import io.ray.streaming.runtime.RayEnvBaseTest;
import java.util.HashMap;
import org.testng.Assert;
import org.testng.annotations.Test;

public class JobMasterTest extends RayEnvBaseTest {

  @Test
  public void testCreation() {
    JobMaster jobMaster = new JobMaster(new HashMap<>());
    Assert.assertNotNull(jobMaster.getRuntimeContext());
    Assert.assertNotNull(jobMaster.getConf());
    Assert.assertNull(jobMaster.getGraphManager());
    Assert.assertNull(jobMaster.getResourceManager());
    Assert.assertNull(jobMaster.getRuntimeContext().getJobMasterActor());
    Assert.assertFalse(jobMaster.init(false));

    jobMaster.destroy();
  }
}
