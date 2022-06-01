package io.ray.streaming.runtime.util.logger;

import io.ray.streaming.common.config.LogConfig;
import java.util.zip.Deflater;
import org.aeonbits.owner.ConfigFactory;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.RollingFileAppender;
import org.apache.logging.log4j.core.appender.rolling.DefaultRolloverStrategy;
import org.apache.logging.log4j.core.appender.rolling.SizeBasedTriggeringPolicy;
import org.apache.logging.log4j.core.config.AppenderRef;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.layout.PatternLayout;

public class Log4j2Utils {

  private static final String STREAMING_APPENDER = "STREAMING_APPENDER";

  public static void initLog4j2Logger(
      Class clazz, String logFileName, int maxBackupIndex, long maxFileSize, String logLevel) {
    final LoggerContext ctx = (LoggerContext) LogManager.getContext();
    final Configuration config = ctx.getConfiguration();
    LogConfig logConfig = ConfigFactory.create(LogConfig.class);

    RollingFileAppender appender = config.getAppender(STREAMING_APPENDER);
    if (appender == null) {
      Layout layout =
          PatternLayout.newBuilder()
              .withPattern(logConfig.logConversionPattern())
              .withConfiguration(config)
              .build();
      SizeBasedTriggeringPolicy policy =
          SizeBasedTriggeringPolicy.createPolicy(Long.toString(maxFileSize));
      DefaultRolloverStrategy strategy =
          DefaultRolloverStrategy.createStrategy(
              Integer.toString(maxBackupIndex),
              null,
              null,
              String.valueOf(Deflater.DEFAULT_COMPRESSION),
              null,
              true,
              config);

      appender =
          RollingFileAppender.createAppender(
              logFileName,
              logFileName + ".%d{yyyy-MM-dd_HH}",
              "false",
              STREAMING_APPENDER,
              null,
              null,
              null,
              policy,
              strategy,
              layout,
              null,
              null,
              null,
              null,
              config);

      appender.start();
      config.addAppender(appender);
    }

    AppenderRef ref =
        AppenderRef.createAppenderRef(STREAMING_APPENDER, Level.toLevel(logLevel), null);
    AppenderRef[] refs = new AppenderRef[] {ref};
    LoggerConfig loggerConfig =
        LoggerConfig.createLogger(
            false, Level.toLevel(logLevel), clazz.getName(), "true", refs, null, config, null);
    loggerConfig.addAppender(appender, null, null);
    config.addLogger(clazz.getName(), loggerConfig);
    ctx.updateLoggers();
  }
}
