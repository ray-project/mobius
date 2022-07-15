package io.ray.streaming.runtime.worker.tasks;

import io.ray.streaming.common.metric.MetricGroup;
import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;
import io.ray.streaming.runtime.util.MetricsUtils;
import io.ray.streaming.state.api.desc.MapStateDescriptor;
import io.ray.streaming.state.api.desc.ValueStateDescriptor;
import io.ray.streaming.state.api.state.MapState;
import io.ray.streaming.state.api.state.ValueState;
import io.ray.streaming.state.manager.StateManager;
import java.util.HashMap;
import java.util.Map;

/**
 * Use Ray to implement RuntimeContext.
 * Different from the {@link io.ray.streaming.runtime.worker.context.JobWorkerContext} that describes
 * system runtime control related information, such as jobName, actorName, vertexMap, OperatorType, etc.
 * this {@link StreamingTaskRuntimeContext} describes computing related information,
 * such as {@link #taskParallelism}, {@link #lastCheckpointId}, {@link #stateManager}, etc.
 */
public class StreamingTaskRuntimeContext implements InternalRuntimeContext {

  private int taskId;
  private int taskIndex;
  private int taskParallelism;
  private int operatorId;
  private String operatorName;
  private Map<String, String> jobConfig;
  private Map<String, String> opConfig;

  private Object currentKey;

  private long lastCheckpointId;
  private MetricGroup metricGroup;

  /**
   * Ray state.
   */
  protected transient StateManager stateManager;

  public StreamingTaskRuntimeContext(ExecutionVertex executionVertex, long checkpointId) {
    this.taskId = executionVertex.getExecutionVertexId();
    this.taskIndex = executionVertex.getExecutionVertexIndex();
    this.taskParallelism = executionVertex.getParallelism();
    this.operatorId = executionVertex.getExecutionJobVertexId();
    this.operatorName = executionVertex.getExecutionJobVertexName();
    this.jobConfig = executionVertex.getJobConfig();
    this.opConfig = executionVertex.getOpConfig();
    this.lastCheckpointId = checkpointId;

    this.metricGroup = MetricsUtils.getMetricGroup(getMetricGroupConfig());
  }

  private Map<String, String> getMetricGroupConfig() {
    Map<String, String>  metricGroupConfig = new HashMap<>();
    metricGroupConfig.putAll(jobConfig);
    metricGroupConfig.putAll(opConfig);
    return metricGroupConfig;
  }

  @Override
  public int getTaskId() {
    return taskId;
  }

  @Override
  public int getTaskIndex() {
    return taskIndex;
  }

  @Override
  public int getTaskParallelism() {
    return taskParallelism;
  }

  @Override
  public int getOperatorId() {
    return operatorId;
  }

  @Override
  public String getOperatorName() {
    return operatorName;
  }

  @Override
  public Map<String, String> getJobConfig() {
    return jobConfig;
  }

  @Override
  public Map<String, String> getOpConfig() {
    if (opConfig != null) {
      return opConfig;
    }
    return new HashMap<>();
  }

  @Override
  public StateManager getStateManager() {
    return stateManager;
  }

  @Override
  public void setStateManager(StateManager stateManager) {
    this.stateManager = stateManager;
  }

  @Override
  public Object getCurrentKey() {
    return this.currentKey;
  }

  @Override
  public void setCurrentKey(Object currentKey) {
    this.currentKey = currentKey;
    stateManager.setCurrentKey(currentKey);
  }

  @Override
  public <V> ValueState<V> getValueState(ValueStateDescriptor<V> stateDescriptor) {
    return this.stateManager.getValueState(stateDescriptor);
  }

  @Override
  public <V> ValueState<V> getNonKeyedValueState(ValueStateDescriptor<V> stateDescriptor) {
    return this.stateManager.getNonKeyedValueState(stateDescriptor);
  }

  @Override
  public <K, V> MapState<K, V> getMapState(
      MapStateDescriptor<K, V> stateDescriptor) {
    return this.stateManager.getMapState(stateDescriptor);
  }

  @Override
  public <K, V> MapState<K, V> getNonKeyedMapState(
      MapStateDescriptor<K, V> stateDescriptor) {
    return this.stateManager.getNonKeyedMapState(stateDescriptor);
  }

  @Override
  public long getCheckpointId() {
    return lastCheckpointId;
  }

  @Override
  public void updateCheckpointId(long checkpointId) {
    this.lastCheckpointId = checkpointId;
  }

  @Override
  public MetricGroup getMetric() {
    return metricGroup;
  }
}
