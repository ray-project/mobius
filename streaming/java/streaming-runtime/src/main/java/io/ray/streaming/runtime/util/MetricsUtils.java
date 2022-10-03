package io.ray.streaming.runtime.util;

import io.ray.streaming.common.config.CommonConfig;
import io.ray.streaming.common.metric.MetricGroup;
import io.ray.streaming.common.metric.local.LocalMetricGroup;
import io.ray.streaming.common.metric.ray.RayMetricGroup;
import io.ray.streaming.common.utils.EnvUtil;
import io.ray.streaming.runtime.config.global.MetricConfig;
import io.ray.streaming.runtime.config.worker.WorkerConfig;
import java.util.HashMap;
import java.util.Map;

public class MetricsUtils {

  public static MetricGroup getMetricGroup(Map<String, String> config) {
    MetricGroup metricGroup;
    if (RayUtils.isInClusterMode()) {
      // We assume default reporter will be ray metric stats module.
      String serviceName = config.get(MetricConfig.METRICS_TSDB_SERVICE_NAME);
      metricGroup = new RayMetricGroup(serviceName, buildMetricGlobalTags(config));
    } else {
      metricGroup = new LocalMetricGroup();
    }
    metricGroup.init(config);

    return metricGroup;
  }

  private static Map<String, String> buildMetricGlobalTags(Map<String, String> jobConfig) {
    Map<String, String> globalTags = new HashMap<>();
    String jobName = jobConfig.get(CommonConfig.JOB_NAME);
    globalTags.put("jobname", jobName);
    globalTags.put("index", jobConfig.get(WorkerConfig.WORKER_ID_INTERNAL));
    globalTags.put(
        "opName", jobConfig.getOrDefault(WorkerConfig.WORKER_OP_NAME_INTERNAL, "default"));
    // a Kepler config key TODO: where is it used for ? need to replace ?
    //        globalTags.put("opId", jobConfig.get(ConfigKey.OPERATOR_IDENTIFY));
    globalTags.put("hostname", EnvUtil.getHostName());
    return globalTags;
  }
}
