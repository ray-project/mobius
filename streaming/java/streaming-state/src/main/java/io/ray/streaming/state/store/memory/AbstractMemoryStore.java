package io.ray.streaming.state.store.memory;

import io.ray.streaming.common.metric.MetricGroup;
import io.ray.streaming.state.store.Store;

public class AbstractMemoryStore implements Store {
  //
  //  protected MetricGroup metricGroup;
  //  protected Meter writeMeter;
  //  protected Meter readMeter;
  //  protected Meter deleteMeter;

  public AbstractMemoryStore(String jobName, String stateName, MetricGroup metricGroup) {
    //    this.metricGroup = metricGroup;
    //    Map<String, String> metricTags = ImmutableMap.of("stateName", stateName);
    //    this.writeMeter = metricGroup.getMeter(MetricConstant.STATE_WRITE_COUNT_METRIC_NAME,
    // metricTags);
    //    this.readMeter = metricGroup.getMeter(MetricConstant.STATE_READ_COUNT_METRIC_NAME,
    // metricTags);
    //    this.deleteMeter = metricGroup.getMeter(MetricConstant.STATE_DELETE_COUNT_METRIC_NAME,
    // metricTags);
  }
}
