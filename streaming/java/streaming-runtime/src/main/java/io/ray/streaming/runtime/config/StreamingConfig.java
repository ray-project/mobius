package io.ray.streaming.runtime.config;

import java.io.Serializable;
import java.util.Map;

/** Streaming config including general, master and worker part. */
public class StreamingConfig implements Serializable {

  private final Map<String, String> userConfig;

  private StreamingMasterConfig masterConfig;
  private StreamingWorkerConfig workerConfigTemplate;

  public StreamingConfig(Map<String, String> conf) {
    userConfig = conf;
    reload();
  }

  public void reload() {
    masterConfig = new StreamingMasterConfig(userConfig);
    workerConfigTemplate = new StreamingWorkerConfig(userConfig);
  }

  /**
   * Incrementally update and reload config among which the root key "training_python_conf", if
   * contains, needs to be parsed separately because it is expressed in the format of nest config
   * string. Theoretically, controlling resource unit is invalid, this is just a temp solution.
   *
   * @param newConf the new config map that only contains required config
   */
  public void updateConfig(Map<String, String> newConf) {
    // All the config must be stored into userConfig in order to be effect after reboot !!
    // Because all the other properties, no matter the Owner properties and configMap, are set to
    // transient!
    userConfig.putAll(newConf);
    reload();
  }

  public Map<String, String> getUserConfig() {
    return userConfig;
  }

  public StreamingMasterConfig getMasterConfig() {
    reloadIfEmpty();
    return masterConfig;
  }

  public StreamingWorkerConfig getWorkerConfigTemplate() {
    reloadIfEmpty();
    return workerConfigTemplate;
  }

  public Map<String, String> getMap() {
    reloadIfEmpty();
    Map<String, String> wholeConfigMap = masterConfig.configMap;
    wholeConfigMap.putAll(workerConfigTemplate.configMap);
    return wholeConfigMap;
  }

  public boolean isEmpty() {
    return masterConfig == null || workerConfigTemplate == null;
  }

  public void reloadIfEmpty() {
    if (isEmpty()) {
      reload();
    }
  }
}
