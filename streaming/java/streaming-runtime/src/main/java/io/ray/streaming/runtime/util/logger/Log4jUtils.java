package io.ray.streaming.runtime.util.logger;

import io.ray.streaming.common.config.LogConfig;
import io.ray.streaming.common.utils.TestHelper;
import io.ray.streaming.runtime.util.ModuleNameAppender;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import org.aeonbits.owner.ConfigFactory;
import org.apache.log4j.Appender;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

public class Log4jUtils {

  public static final int LOG_BUFFER_SIZE = 8192;

  private static ConcurrentMap<String, FileWriter> writers = new ConcurrentHashMap<>();

  private static FileWriter createWriter(String fileName) {

    return writers.computeIfAbsent(
        fileName,
        new Function<String, FileWriter>() {
          @Override
          public FileWriter apply(String s) {
            try {
              return new FileWriter(fileName);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        });
  }

  public static void initLog4jLogger(
      Class clazz, String loggerFileName, int maxBackupIndex, long maxFileSize, String logLevel) {
    Logger logger = Logger.getLogger(clazz);
    LogConfig logConfig = ConfigFactory.create(LogConfig.class);

    PatternLayout layout = new PatternLayout(logConfig.logLayoutPattern());
    layout.setConversionPattern(logConfig.logConversionPattern());

    String logEncoding = System.getProperty("file.encoding");
    if (logEncoding == null || logEncoding.isEmpty()) {
      logEncoding = "UTF-8";
    }

    try {
      Appender appender;
      if (TestHelper.isUTPattern()) {
        appender = new ConsoleAppender(layout, ConsoleAppender.SYSTEM_OUT);
      } else {
        ModuleNameAppender fileAppender = new ModuleNameAppender();
        fileAppender.setLayout(layout);
        fileAppender.setMaxBackupIndex(maxBackupIndex);
        fileAppender.setMaximumFileSize(maxFileSize);
        fileAppender.setEncoding(logEncoding);
        fileAppender.setName("file");
        fileAppender.setWriter(createWriter(loggerFileName));
        fileAppender.setFile(loggerFileName, true, true, LOG_BUFFER_SIZE);
        appender = fileAppender;
      }

      logger.addAppender(appender);
      logger.setLevel(Level.toLevel(logLevel));
      logger.setAdditivity(false);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
