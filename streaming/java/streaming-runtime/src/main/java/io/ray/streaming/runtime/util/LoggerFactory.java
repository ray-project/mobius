package io.ray.streaming.runtime.util;

import io.ray.streaming.common.config.LogConfig;
import io.ray.streaming.runtime.util.logger.Log4j2Utils;
import io.ray.streaming.runtime.util.logger.Log4jUtils;
import java.io.File;
import java.lang.management.ManagementFactory;
import org.aeonbits.owner.ConfigFactory;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;

public class LoggerFactory {

  private static final String LOGGER_FACTORY_LOG4J = "org.slf4j.memory.Log4jLoggerFactory";
  private static final String LOGGER_FACTORY_LOG4J2 = "org.apache.logging.slf4j.Log4jLoggerFactory";
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
    ILoggerFactory iLoggerFactory = org.slf4j.LoggerFactory.getILoggerFactory();

    if (LOGGER_FACTORY_LOG4J.equals(iLoggerFactory.getClass().getName())) {
      // log4j
      Log4jUtils.initLog4jLogger(
          clazz, getLogFileName(), getMaxBackupIndex(), getMaxFileSize(), getLogLevel());
    } else if (LOGGER_FACTORY_LOG4J2.equals(iLoggerFactory.getClass().getName())) {
      // log4j2
      Log4j2Utils.initLog4j2Logger(
          clazz, getLogFileName(), getMaxBackupIndex(), getMaxFileSize(), getLogLevel());
    } else {
      throw new RuntimeException("Unsupported logger factory " + iLoggerFactory + "!");
    }
    return iLoggerFactory.getLogger(clazz.getName());
  }

  //  public static void updateLogLevel(String logLevel) {
  //    ILoggerFactory iLoggerFactory = org.slf4j.LoggerFactory.getILoggerFactory();
  //    if (LOGGER_FACTORY_LOG4J.equals(iLoggerFactory.getClass().getName())) {
  //      Level level = Level.toLevel(logLevel);
  //      LogManager.getRootLogger().setLevel(level);
  //    } else {
  //      throw new RuntimeException("Unsupported log level changing besides log4j and log4j2.");
  //    }
  //  }

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
