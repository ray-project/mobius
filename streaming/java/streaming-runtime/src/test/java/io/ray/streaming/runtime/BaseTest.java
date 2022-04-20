package io.ray.streaming.runtime;

import io.ray.api.Ray;
import io.ray.streaming.runtime.util.TestHelper;
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
  protected boolean isClusterMode;
  protected String jobName;

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

  @BeforeMethod(alwaysRun = true)
  public void testBegin(Method method) {
    LOG.info(
        ">>>>>>>>>>>>>>>>>>>> Test case(Cluster mode: {}): {}.{} began >>>>>>>>>>>>>>>>>>>>",
        isClusterMode,
        method.getDeclaringClass(),
        method.getName());
    if (isClusterMode) {
      System.setProperty(RAY_MODE, RAY_CLUSTER_MODE);
      System.setProperty("ray.job.jvm-options.0", "-DUT_PATTERN=true");
      System.setProperty("ray.redirect-output", "true");
    }
    if (Ray.isInitialized()) {
      Ray.shutdown();
    }
    Assert.assertFalse(Ray.isInitialized());

    Ray.init();
    TestHelper.setUTFlag();
    rayAsyncContext = Ray.getAsyncContext();

    jobName = TestHelper.getTestName(this, method);
    TestHelper.cleanUpJobRunningRubbish(jobName);
  }

  @AfterMethod(alwaysRun = true)
  public void testEnd(Method method) {
    if (!isClusterMode) {
      String testName = TestHelper.getTestName(this, method);
      try {
        TestHelper.destroyUTJobMasterByJobName(testName);
      } catch (Exception e) {
        LOG.warn("Error when destroying job master for test: {}.", testName);
      }
    }

    TestHelper.clearUTFlag();
    Ray.shutdown();
    if (isClusterMode) {
      System.clearProperty(RAY_MODE);
    }
    LOG.info(
        ">>>>>>>>>>>>>>>>>>>> Test case: {}.{} end >>>>>>>>>>>>>>>>>>>>",
        method.getDeclaringClass(),
        method.getName());
  }
}
