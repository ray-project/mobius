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

/** Basic implements for test which need ray environment. */
public abstract class RayEnvBaseTest {

  private static final Logger LOG = LoggerFactory.getLogger(RayEnvBaseTest.class);

  private static final String RAY_MODE = "ray.run-mode";
  private static final String RAY_CLUSTER_MODE = "CLUSTER";

  protected transient Object rayAsyncContext;
  protected boolean isClusterMode;
  protected String jobName;

  public RayEnvBaseTest(boolean isClusterMode) {
    this.isClusterMode = isClusterMode;
  }

  public RayEnvBaseTest() {
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
    if (Ray.isInitialized()) {
      Ray.shutdown();
    }
    Assert.assertFalse(Ray.isInitialized());

    jobName = TestHelper.getTestName(this, method);
    LOG.info(
        ">>>>>>>>>>>>>>>>>>>> Test case(Cluster mode: {}): {} began >>>>>>>>>>>>>>>>>>>>",
        isClusterMode,
        jobName);

    if (isClusterMode) {
      System.setProperty(RAY_MODE, RAY_CLUSTER_MODE);
      System.setProperty("ray.job.jvm-options.0", "-DUT_PATTERN=true");
      System.setProperty("ray.redirect-output", "true");
    }

    Ray.init();
    TestHelper.setUTFlag();
    rayAsyncContext = Ray.getAsyncContext();

    TestHelper.cleanUpJobRunningRubbish(jobName);
  }

  @AfterMethod(alwaysRun = true)
  public void testEnd(Method method) {
    String jobName = TestHelper.getTestName(this, method);
    if (!isClusterMode) {
      try {
        TestHelper.destroyUTJobMasterByJobName(jobName);
      } catch (Exception e) {
        LOG.warn("Error when destroying job master for test: {}.", jobName, e);
      }
    }

    TestHelper.clearUTFlag();
    Ray.shutdown();
    if (isClusterMode) {
      System.clearProperty(RAY_MODE);
    }
    LOG.info(">>>>>>>>>>>>>>>>>>>> Test case: {} end >>>>>>>>>>>>>>>>>>>>", jobName);
  }
}
