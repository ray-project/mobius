package io.ray.sreaming.common.utils;

import io.ray.sreaming.common.config.Config;
import java.lang.reflect.Field;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Common tools.
 */
public class CommonUtil {

  private static final Logger LOG = LoggerFactory.getLogger(CommonUtil.class);

  public static Map<String, Object> stringMapToObjectMap(Map<String, String> sourceMap) {
    Map<String, Object> resultMap = (Map) sourceMap;
    return resultMap;
  }

  public static int gcd(int a, int b) {
    return a % b == 0 ? b : gcd(b, a % b);
  }

  public static int min(int... nums) {
    int minNum = nums[0];
    for(int i = 1; i < nums.length; i++) {
      minNum = Math.min(minNum, nums[i]);
    }
    return minNum;
  }

  public static Map<String, String> objectMapToStringMap(Map<String, Object> sourceMap) {
    return sourceMap.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> {
          if (e.getValue() instanceof String) {
            return (String) e.getValue();
          } else if (e.getValue() == null) {
            LOG.warn("Invalid value for key: {}.", e.getKey());
            return "";
          } else {
            return e.getValue().toString();
          }
        }));
  }

  public static void ignore(Object... args) {
  }

  public static String getStackMsg(Exception e) {
    StringBuffer sb = new StringBuffer();
    StackTraceElement[] stackArray = e.getStackTrace();

    sb.append(e.getMessage() + "\n");
    for (int i = 0; i < stackArray.length; i++) {
      StackTraceElement element = stackArray[i];
      sb.append(element.toString() + "\n");
    }
    return sb.toString();
  }

  /**
   * Get config keys from config interface.
   *
   * @param clazz config interface
   * @return config keys
   */
  public static List<String> getConfigKeys(Class<? extends Config> clazz) {
    List<String> configKeys = new ArrayList<>();

    try {
      for (Field field : clazz.getFields()) {
        Object value = field.get(null);
        if (value instanceof String) {
          configKeys.add((String) value);
        }
      }
    } catch (Exception e) {
      return null;
    }
    return configKeys;
  }

  /**
   * Print partial barrier content in string format.
   * @param globalCheckpointId global checkpoint id for partial barrier
   * @param partialCheckpointId partial checkpoint id for partial barrier
   * @return string
   */
  public static String genPartialBarrierStr(long globalCheckpointId, long partialCheckpointId) {
    return globalCheckpointId + "-" + partialCheckpointId;
  }

  /**
   * Judge whether the input time is expired.
   * true: time < now
   * false: time >= now
   * @param time target time in long value
   * @return result
   */
  public static boolean isTimeExpired(long time) {
    long now = System.currentTimeMillis();
    int nowTimeLength = Long.toString(now).length();
    int timeLength = Long.toString(time).length();

    if (timeLength < nowTimeLength) {
      time = (long) (time * Math.pow(10, nowTimeLength - timeLength));
    } else if (timeLength > nowTimeLength) {
      time = (long) (time / Math.pow(10, nowTimeLength - timeLength));
    }

    if (time >= now) {
      return false;
    }
    return true;
  }

  /**
   * Format double number into readable string format.
   *
   * @param number the number need to be format
   * @param decimalNum haw many decimals should place
   * @return number after formated
   */
  public static String formatDoubleWithSpecifiedDecimals(Double number, int decimalNum) {
    StringBuffer pattern;
    if (decimalNum <= 0) {
      pattern = new StringBuffer("#");
    } else {
      pattern = new StringBuffer("#.");
      for (int i = 0; i < decimalNum; i++) {
        pattern.append("#");
      }
    }

    DecimalFormat decimalFormat = new DecimalFormat(pattern.toString());
    return decimalFormat.format(number);
  }

  /**
   * Is classA a subclass of classB.
   *
   * @param clazzNameA classA' name in string
   * @param clazzNameB classB' name in string
   * @return result
   */
  public static boolean isSubClazz(String clazzNameA, String clazzNameB) {
    try {
      return (Class.forName(clazzNameB)).isAssignableFrom(Class.forName(clazzNameA));
    } catch (Exception e) {
      return false;
    }
  }
}
