package io.ray.streaming.runtime.util;

import io.ray.streaming.common.config.LogConfig;
import io.ray.streaming.runtime.util.logger.Log4j2Utils;
import java.io.File;
import java.lang.management.ManagementFactory;
import org.aeonbits.owner.ConfigFactory;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;

public class LoggerFactory {

  private static final LogConfig LOG_CONFIG = ConfigFactory.create(LogConfig.class);

  public static String getLogPath() {
    String logDir = System.getenv(LOG_CONFIG.logDirEnvKey());
    if (null == logDir) {
      logDir = LOG_CONFIG.logRootDir();
    }
    return logDir;
  }

  public static String getLogFileName() {
    String logDir = getLogPath();
    File dir = new File(logDir);
    if (!dir.exists()) {
      dir.mkdirs();
    }

    String logFile = logDir + File.separator + LOG_CONFIG.logFilePattern();

    String pid = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
    return String.format(logFile, pid);
  }

  public static Logger getLogger(Class clazz) {
    ILoggerFactory loggerFactory = org.slf4j.LoggerFactory.getILoggerFactory();
    Log4j2Utils.initLog4j2Logger(
        clazz, getLogFileName(), getMaxBackupIndex(), getMaxFileSize(), getLogLevel());
    return loggerFactory.getLogger(clazz.getName());
  }

  private static String getLogLevel() {
    String logLevelStr = System.getenv(LOG_CONFIG.logLevelEnvKey());
    if (StringUtils.isEmpty(logLevelStr)) {
      logLevelStr = LOG_CONFIG.logLevel();
    }
    return logLevelStr;
  }

  private static int getMaxBackupIndex() {
    String maxBackupCountStr = System.getenv(LOG_CONFIG.logBackupCountEnvKey());
    if (StringUtils.isEmpty(maxBackupCountStr)) {
      return LOG_CONFIG.logBackupCount();
    }
    return Integer.valueOf(maxBackupCountStr);
  }

  private static long getMaxFileSize() {
    String maxFileSizeFromEnv = System.getenv(LOG_CONFIG.logMaxBytesEnvKey());
    String maxFileSizeFromProp = System.getProperty(LOG_CONFIG.logMaxBytesEnvKey());

    if (StringUtils.isEmpty(maxFileSizeFromEnv) && StringUtils.isEmpty(maxFileSizeFromProp)) {
      return LOG_CONFIG.logMaxBytes();
    } else if (StringUtils.isEmpty(maxFileSizeFromEnv)
        && !StringUtils.isEmpty(maxFileSizeFromProp)) {
      return Long.parseLong(maxFileSizeFromProp);
    }
    return Long.parseLong(maxFileSizeFromEnv);
  }
}
