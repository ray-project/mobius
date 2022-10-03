package io.ray.streaming.api.context;

import io.ray.streaming.common.metric.MetricGroup;
import io.ray.streaming.state.api.desc.MapStateDescriptor;
import io.ray.streaming.state.api.desc.ValueStateDescriptor;
import io.ray.streaming.state.api.state.MapState;
import io.ray.streaming.state.api.state.ValueState;
import java.util.Map;

/** Encapsulate the runtime information of a streaming task. */
public interface RuntimeContext {

  /**
   * Get task's unique id.
   *
   * @return task unique id
   */
  int getTaskId();

  /**
   * Get task's sub index.
   *
   * @return task sub index
   */
  int getTaskIndex();

  /**
   * Get current task's group's parallelism.
   *
   * @return parallelism of the task's group
   */
  int getTaskParallelism();

  /**
   * Get operator id(task group's unique id).
   *
   * @return operator id
   */
  int getOperatorId();

  /**
   * Get operator name. e.g. 1-SourceOperator, 3-MapOperator
   *
   * @return operator name
   */
  String getOperatorName();

  /** Returns config of job. */
  Map<String, String> getJobConfig();

  /** Returns config of operator. */
  Map<String, String> getOpConfig();

  /**
   * Get user defined key value.
   *
   * @return key
   */
  Object getCurrentKey();

  /**
   * Set user defined key value.
   *
   * @param currentKey key value
   */
  void setCurrentKey(Object currentKey);

  /**
   * Get state of value type.
   *
   * @param stateDescriptor state descriptor
   * @param <V> type of state's value
   * @return state
   */
  <V> ValueState<V> getValueState(ValueStateDescriptor<V> stateDescriptor);

  /**
   * Get state of value type(nonKeyed).
   *
   * @param stateDescriptor state descriptor
   * @param <V> type of state's value
   * @return state
   */
  <V> ValueState<V> getNonKeyedValueState(ValueStateDescriptor<V> stateDescriptor);

  /**
   * Get state of key-value type.
   *
   * @param stateDescriptor state descriptor
   * @param <K> type of state's key
   * @param <V> type of state's value
   * @return state
   */
  <K, V> MapState<K, V> getMapState(MapStateDescriptor<K, V> stateDescriptor);

  /**
   * Get state of key-value type(nonKeyed).
   *
   * @param stateDescriptor state descriptor
   * @param <K> type of state's key
   * @param <V> type of state's value
   * @return state
   */
  <K, V> MapState<K, V> getNonKeyedMapState(MapStateDescriptor<K, V> stateDescriptor);

  /**
   * Get the current checkpoint of the runtime.
   *
   * @return checkpoint id.
   */
  long getCheckpointId();

  /**
   * Get metric object.
   *
   * @return metric group
   */
  MetricGroup getMetric();
}
