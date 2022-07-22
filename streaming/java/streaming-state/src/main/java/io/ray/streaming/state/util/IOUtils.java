package io.ray.streaming.state.util;

import java.io.File;

public class IOUtils {

  public static void closeQuietly(AutoCloseable closeable) {
    try {
      if (closeable != null) {
        closeable.close();
      }
    } catch (Throwable ignored) {
      // ignore exception
    }
  }

  public static boolean forceDeleteFile(File file) {
    if (!file.exists()) {
      return false;
    }

    if (file.isDirectory()) {
      File[] files = file.listFiles();
      for (File f : files) {
        forceDeleteFile(f);
      }
    }
    return file.delete();
  }
}
