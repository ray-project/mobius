package io.ray.streaming.runtime.util;

import io.ray.streaming.runtime.config.global.CommonConfig;
import io.ray.streaming.runtime.master.JobMaster;
import java.io.File;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import org.aeonbits.owner.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utils single-process test case. */
public class TestHelper {

  private static final Logger LOG = LoggerFactory.getLogger(TestHelper.class);

  public static final String REST_SERVER_INFO_PATH_PREFIX = "/tmp/restServerAddress_";
  private static final Map<String, JobMaster> UT_JOB_MASTER_MAP = new HashMap<>();
  private static final String JOB_NAME_DEFAULT = ConfigFactory.create(CommonConfig.class).jobName();
  private static volatile boolean UT_FLAG = false;

  public static void setUTFlag() {
    UT_FLAG = true;
  }

  public static void clearUTFlag() {
    UT_FLAG = false;
  }

  public static boolean isUT() {
    return UT_FLAG;
  }

  public static String getTestName(Object clazz, Method method) {
    return clazz.getClass().getName() + "_" + method.getName();
  }

  public static String getRestServerInfoFilePath(String jobName) {
    return REST_SERVER_INFO_PATH_PREFIX + jobName;
  }

  public static boolean checkRestServerInfoFileExist(String jobName) {
    File file = new File(getRestServerInfoFilePath(jobName));
    return file.exists() && file.isFile();
  }

  public static void deleteRestServerInfoFile(String jobName) {
    File file = new File(getRestServerInfoFilePath(jobName));
    if (file.exists() && file.isFile()) {
      file.delete();
    }
  }

  public static void cleanUpJobRunningRubbish(String jobName) {
    deleteRestServerInfoFile(jobName);
  }

  public static JobMaster getUTJobMasterByJobName(String jobName) {
    return UT_JOB_MASTER_MAP.get(jobName);
  }

  public static void setUTJobMasterByJobName(String jobName, JobMaster jobMaster) {
    LOG.info("Set {}'s job-master in ut map.", jobName);
    UT_JOB_MASTER_MAP.put(jobName, jobMaster);
  }

  public static void destroyUTJobMasterByJobName(String jobname) {
    LOG.info(
        "Before destroy job master for ut test: {}, ut-jobmaster-map is: {}.",
        jobname,
        UT_JOB_MASTER_MAP.keySet());

    JobMaster jobMaster = UT_JOB_MASTER_MAP.get(jobname);
    if (null != jobMaster) {
      boolean result = jobMaster.destroy();
      if (result) {
        UT_JOB_MASTER_MAP.remove(jobname);
        LOG.info("Finished destroying job master for ut test: {}.", jobname);
      } else {
        LOG.error("Failed to destroy job master for ut test: {}.", jobname);
      }
    } else {
      LOG.warn("ExecutionGraphTes: {}.", jobname);
    }

    LOG.info(
        "After destroy job master for ut test: {}, ut-jobmaster-map is: {}.",
        jobname,
        UT_JOB_MASTER_MAP.keySet());

    if (UT_JOB_MASTER_MAP.containsKey(JOB_NAME_DEFAULT)) {
      jobMaster = UT_JOB_MASTER_MAP.get(JOB_NAME_DEFAULT);
      if (null != jobMaster) {
        jobMaster.destroy();
        LOG.warn("Finished destroying job master for ut test: {}.", JOB_NAME_DEFAULT);
      }
      UT_JOB_MASTER_MAP.remove(jobname);
    }
  }
}
