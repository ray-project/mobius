package io.ray.streaming.common.metric;

import java.io.Serializable;
import java.util.Map;

public interface MetricGroup extends Serializable {

  default void init(Map<String, String> jobConfig) {}

  Gauge getGauge(String gaugeName);

  Gauge getGauge(String gaugeName, Map<String, String> tags);

  Meter getMeter(String meterName);

  Meter getMeter(String meterName, Map<String, String> tags);

  Counter getCounter(String counterName);

  Counter getCounter(String counterName, Map<String, String> tags);

  Histogram getHistogram(String histogramName);

  Histogram getHistogram(String histogramName, Map<String, String> tags);
}
