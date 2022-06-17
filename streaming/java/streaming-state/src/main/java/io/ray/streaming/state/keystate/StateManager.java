package io.ray.streaming.state.keystate;

import io.ray.streaming.common.metric.MetricGroup;
import io.ray.streaming.state.keystate.desc.KeyMapStateDescriptor;
import io.ray.streaming.state.keystate.desc.KeyValueStateDescriptor;
import io.ray.streaming.state.keystate.desc.ValueStateDescriptor;
import io.ray.streaming.state.keystate.state.KeyMapState;
import io.ray.streaming.state.keystate.state.KeyValueState;
import io.ray.streaming.state.keystate.state.State;
import io.ray.streaming.state.keystate.state.ValueState;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * StateManager is the entrance to the state, through the StateManager instance can get
 * ValueState, ListState, KeyValueState,KeyMapState, etc.
 */
public class StateManager extends AbstractStateManager implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(StateManager.class);

  private Map<String, State> stateMap = new HashMap<>();

  public StateManager(final Map<String, String> config,
                      final MetricGroup metricGroup) {
    this("default", "default", 1, 0, config, metricGroup);
  }

  public StateManager(final String jobName,
                      final String stateName,
                      final Map<String, String> config,
                      final MetricGroup metricGroup) {

    this(jobName, stateName, 1, 0, config, metricGroup);
  }

  public StateManager(final String jobName,
                      final String stateName,
                      final int taskParallelism,
                      final int taskIndex,
                      final Map<String, String> config,
                      final MetricGroup metricGroup) {
    super(jobName, stateName, taskParallelism, taskIndex, config, metricGroup);

    LOG.info("StateManager init success, jobName: {}, stateName: {}, taskParallelism: {}, " +
            "taskIndex: {}, maxParallelism: {}, state config: {}.",
            jobName, operatorName, taskParallelism, taskIndex, maxShard, config);
  }

  @Override
  public <K, V> KeyValueState<K, V> getKeyValueState(
      KeyValueStateDescriptor<K, V> stateDescriptor) {
    LOG.info("Get key value state, descriptor: {}, state map: {}.", stateDescriptor, stateMap);
    if (stateMap.containsKey(stateDescriptor.getStateName())) {
      return (KeyValueState<K, V>) stateMap.get(stateDescriptor.getStateName());
    }
    //laze create store backend.
    buildStoreManager();
    KeyValueState<K, V> state = stateStoreManager.buildKeyValueStore(stateDescriptor);
    stateMap.put(stateDescriptor.getStateName(), state);
    return state;
  }

  @Override
  public <K, V> KeyValueState<K, V> getNonKeyedKeyValueState(
      KeyValueStateDescriptor<K, V> stateDescriptor) {
    LOG.info("Get nonkeyed key value state, descriptor: {}, state map: {}.", stateDescriptor, stateMap);
    if (stateMap.containsKey(stateDescriptor.getStateName())) {
      return (KeyValueState<K, V>) stateMap.get(stateDescriptor.getStateName());
    }
    //laze create store backend.
    buildStoreManager();
    KeyValueState<K, V> state = stateStoreManager.buildNonKeyedKeyValueStore(stateDescriptor);
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
    ValueState<V> state = stateStoreManager.buildValueStore(stateDescriptor);
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
    ValueState<V> state = stateStoreManager.buildNonKeyedValueStore(stateDescriptor);
    stateMap.put(stateDescriptor.getStateName(), state);
    return state;
  }

  @Override
  public <K, UK, UV> KeyMapState<K, UK, UV> getKeyMapState(
      KeyMapStateDescriptor<K, UK, UV> stateDescriptor) {

    LOG.info("Get key map state, descriptor: {}, state map: {}.", stateDescriptor, stateMap);
    if (stateMap.containsKey(stateDescriptor.getStateName())) {
      return (KeyMapState<K, UK, UV>) stateMap.get(stateDescriptor.getStateName());
    }
    //laze create store backend.
    buildStoreManager();
    KeyMapState<K, UK, UV> state = stateStoreManager.buildKeyMapStore(stateDescriptor);
    stateMap.put(stateDescriptor.getStateName(), state);
    return state;
  }

  @Override
  public Map<String, State> getStateMap() {
    return stateMap;
  }
}