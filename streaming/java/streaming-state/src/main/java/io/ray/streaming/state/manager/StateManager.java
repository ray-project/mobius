package io.ray.streaming.state.manager;

import io.ray.streaming.common.metric.MetricGroup;
import io.ray.streaming.state.api.desc.ListStateDescriptor;
import io.ray.streaming.state.api.desc.MapStateDescriptor;
import io.ray.streaming.state.api.desc.ValueStateDescriptor;
import io.ray.streaming.state.api.state.ListState;
import io.ray.streaming.state.api.state.MapState;
import io.ray.streaming.state.api.state.State;
import io.ray.streaming.state.api.state.ValueState;
import io.ray.streaming.state.typeinfo.serializer.TypeSerializerConfig;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * StateManager is the entrance to the state, through the StateManager instance can get ValueState,
 * ListState, KeyValueState,KeyMapState, etc.
 */
public class StateManager extends AbstractStateManager implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(StateManager.class);

  private Map<String, State> stateMap = new HashMap<>();

  public StateManager(final Map<String, String> config, final MetricGroup metricGroup) {
    this("default", "default", 1, 0, config, metricGroup);
  }

  public StateManager(
      final String jobName,
      final String stateName,
      final Map<String, String> config,
      final MetricGroup metricGroup) {

    this(jobName, stateName, 1, 0, config, metricGroup);
  }

  public StateManager(
      final String jobName,
      final String stateName,
      final int taskParallelism,
      final int taskIndex,
      final Map<String, String> config,
      final MetricGroup metricGroup) {

    this(jobName, stateName, taskParallelism, taskIndex, config, null, metricGroup);
  }

  public StateManager(
      final String jobName,
      final String stateName,
      final int taskParallelism,
      final int taskIndex,
      final Map<String, String> config,
      final TypeSerializerConfig serializerConfig,
      final MetricGroup metricGroup) {
    super(jobName, stateName, taskParallelism, taskIndex, config, serializerConfig, metricGroup);

    LOG.info(
        "StateManager init success, jobName: {}, stateName: {}, taskParallelism: {}, "
            + "taskIndex: {}, maxParallelism: {}, state config: {}.",
        jobName,
        operatorName,
        taskParallelism,
        taskIndex,
        maxShard,
        config);
  }

  @Override
  public <K, V> MapState<K, V> getMapState(MapStateDescriptor<K, V> stateDescriptor) {
    LOG.info("Get key value state, descriptor: {}, state map: {}.", stateDescriptor, stateMap);
    if (stateMap.containsKey(stateDescriptor.getStateName())) {
      return (MapState<K, V>) stateMap.get(stateDescriptor.getStateName());
    }
    // laze create store backend.
    buildStoreManager();
    MapState<K, V> state = storeManager.buildMapStore(stateDescriptor);
    stateMap.put(stateDescriptor.getStateName(), state);
    return state;
  }

  @Override
  public <K, V> MapState<K, V> getNonKeyedMapState(MapStateDescriptor<K, V> stateDescriptor) {
    LOG.info(
        "Get nonkeyed key value state, descriptor: {}, state map: {}.", stateDescriptor, stateMap);
    if (stateMap.containsKey(stateDescriptor.getStateName())) {
      return (MapState<K, V>) stateMap.get(stateDescriptor.getStateName());
    }
    // laze create store backend.
    buildStoreManager();
    MapState<K, V> state = storeManager.buildNonKeyedMapStore(stateDescriptor);
    stateMap.put(stateDescriptor.getStateName(), state);
    return state;
  }

  @Override
  public <V> ValueState<V> getValueState(ValueStateDescriptor<V> stateDescriptor) {
    LOG.info("Get value state, descriptor: {}, state map:{}.", stateDescriptor, stateMap);
    if (stateMap.containsKey(stateDescriptor.getStateName())) {
      return (ValueState<V>) stateMap.get(stateDescriptor.getStateName());
    }

    buildStoreManager();
    ValueState<V> state = storeManager.buildValueStore(stateDescriptor);
    stateMap.put(stateDescriptor.getStateName(), state);
    return state;
  }

  @Override
  public <V> ValueState<V> getNonKeyedValueState(ValueStateDescriptor<V> stateDescriptor) {
    LOG.info("Get nonkeyed value state, descriptor: {}, state map:{}.", stateDescriptor, stateMap);
    if (stateMap.containsKey(stateDescriptor.getStateName())) {
      return (ValueState<V>) stateMap.get(stateDescriptor.getStateName());
    }

    buildStoreManager();
    ValueState<V> state = storeManager.buildNonKeyedValueStore(stateDescriptor);
    stateMap.put(stateDescriptor.getStateName(), state);
    return state;
  }

  @Override
  public <V> ListState<V> getNonKeyedListState(ListStateDescriptor<V> stateDescriptor) {
    LOG.info("Get list state, descriptor: {}, state map: {}.", stateDescriptor, stateMap);
    if (stateMap.containsKey(stateDescriptor.getStateName())) {
      return (ListState<V>) stateMap.get(stateDescriptor.getStateName());
    }

    // laze create store backend.
    buildStoreManager();

    ListState<V> state = storeManager.buildNonKeyedListStore(stateDescriptor);
    stateMap.put(stateDescriptor.getStateName(), state);
    return state;
  }

  @Override
  public Map<String, State> getStateMap() {
    return stateMap;
  }
}
