package io.ray.streaming.runtime.worker.context;

import com.google.common.base.Preconditions;
import io.ray.streaming.api.context.RuntimeContext;
import io.ray.streaming.common.metric.MetricGroup;
import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;
import io.ray.streaming.state.backend.AbstractKeyStateBackend;
import io.ray.streaming.state.backend.KeyStateBackend;
import io.ray.streaming.state.backend.OperatorStateBackend;
import io.ray.streaming.state.keystate.desc.AbstractStateDescriptor;
import io.ray.streaming.state.keystate.desc.ListStateDescriptor;
import io.ray.streaming.state.keystate.desc.MapStateDescriptor;
import io.ray.streaming.state.keystate.desc.ValueStateDescriptor;
import io.ray.streaming.state.keystate.state.ListState;
import io.ray.streaming.state.keystate.state.MapState;
import io.ray.streaming.state.keystate.state.ValueState;
import java.util.HashMap;
import java.util.Map;

/** Use Ray to implement RuntimeContext. */
public class StreamingRuntimeContext implements InternalRuntimeContext {

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

  public StreamRuntimeContext(ExecutionVertex executionVertex, long checkpointId) {
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
  public <K, V> KeyValueState<K, V> getKeyValueState(
      KeyValueStateDescriptor<K, V> stateDescriptor) {
    return this.stateManager.getKeyValueState(stateDescriptor);
  }

  @Override
  public <K, V> KeyValueState<K, V> getNonKeyedKeyValueState(
      KeyValueStateDescriptor<K, V> stateDescriptor) {
    return this.stateManager.getNonKeyedKeyValueState(stateDescriptor);
  }

  @Override
  public <K, UK, UV> KeyMapState<K, UK, UV> getKeyMapState(
      KeyMapStateDescriptor<K, UK, UV> stateDescriptor) {
    return this.stateManager.getKeyMapState(stateDescriptor);
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
