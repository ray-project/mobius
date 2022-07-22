package io.ray.streaming.common.metric;

import java.util.HashMap;
import java.util.Map;

public class MetricPluginUtils {

  /**
   * build full metric name with all tag entries.
   *
   * @param metricName main name
   * @param tags tags of the metric
   * @return full metric name with metric name and tags bounded
   */
  public static String getMetricNameWithTags(String metricName, Map<String, String> tags) {
    StringBuilder sb = new StringBuilder();
    sb.append(metricName);
    tags.entrySet().stream()
        .forEachOrdered(
            x -> {
              sb.append('\n' + x.getKey() + ":" + x.getValue());
            });
    return sb.toString();
  }

  /**
   * Build mixed tags in local priority first.
   * A local tag will override the global tag if it has the same key.
   *
   * @param localTags local tags
   * @param globalTags global tags
   * @return mixed result
   */
  public static Map<String, String> buildMixedTags(
      final Map<String, String> localTags, final Map<String, String> globalTags) {

    Map<String, String> mixedTags = new HashMap<>();
    mixedTags.putAll(globalTags);
    mixedTags.putAll(localTags);
    return mixedTags;
  }
}
