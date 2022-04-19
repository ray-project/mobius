package io.ray.streaming.runtime;

import io.ray.api.Ray;
import java.lang.reflect.Method;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

public abstract class BaseTest {

  private static final Logger LOG = LoggerFactory.getLogger(BaseTest.class);

  private static final String RAY_MODE = "ray.run-mode";
  private static final String RAY_CLUSTER_MODE = "CLUSTER";

  protected transient Object rayAsyncContext;

  private boolean isClusterMode;

  public BaseTest(boolean isClusterMode) {
    this.isClusterMode = isClusterMode;
  }

  public BaseTest() {
    // Default is false!
    this.isClusterMode = false;
  }

  @BeforeClass
  public void setUp() {
    TestHelper.setUTFlag();
  }

  @AfterClass
  public void tearDown() {
    TestHelper.clearUTFlag();
  }

  @BeforeMethod
  public void testBegin(Method method) {
    LOG.info(
        ">>>>>>>>>>>>>>>>>>>> Test case: {}.{} began >>>>>>>>>>>>>>>>>>>>",
        method.getDeclaringClass(),
        method.getName());
    if (this.isClusterMode) {
      LOG.info("Running in Ray cluster mode.");
      System.setProperty(RAY_MODE, RAY_CLUSTER_MODE);
      System.setProperty("ray.job.jvm-options.0", "-DUT_PATTERN=true");
      System.setProperty("ray.session-dir", "/tmp/ray");
    }
    if (Ray.isInitialized()) {
      Ray.shutdown();
    }
    Assert.assertFalse(Ray.isInitialized());

    Ray.init();
    TestHelper.setUTFlag();
    rayAsyncContext = Ray.getAsyncContext();
  }

  @AfterMethod
  public void testEnd(Method method) {
    TestHelper.clearUTFlag();
    Ray.shutdown();
    if (this.isClusterMode) {
      System.clearProperty(RAY_MODE);
    }
    LOG.info(
        ">>>>>>>>>>>>>>>>>>>> Test case: {}.{} end >>>>>>>>>>>>>>>>>>>>",
        method.getDeclaringClass(),
        method.getName());
  }
}
