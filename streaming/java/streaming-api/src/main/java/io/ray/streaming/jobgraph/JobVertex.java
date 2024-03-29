package io.ray.streaming.jobgraph;

import com.google.common.base.MoreObjects;
import io.ray.streaming.api.Language;
import io.ray.streaming.operator.AbstractStreamOperator;
import io.ray.streaming.operator.StreamOperator;
import io.ray.streaming.operator.chain.ChainedOperator;
import io.ray.streaming.python.PythonOperator;
import java.io.Serializable;
import java.util.Map;

/** Job vertex is a cell node where logic is executed. */
public class JobVertex implements Serializable {

  private int vertexId;

  private final int dynamicDivisionNum;
  private int parallelism;
  private VertexType vertexType;
  private Language language;
  private StreamOperator operator;
  private Map<String, String> config;

  public JobVertex(
      int vertexId,
      int parallelism,
      int dynamicDivisionNum,
      VertexType vertexType,
      AbstractStreamOperator<?> operator) {
    this.vertexId = vertexId;
    this.parallelism = parallelism;
    this.dynamicDivisionNum = dynamicDivisionNum;
    this.vertexType = vertexType;
    this.operator = operator;
    this.language = operator.getLanguage();
    operator.setId(vertexId);
  }

  public int getVertexId() {
    return vertexId;
  }

  public int getParallelism() {
    return parallelism;
  }

  public StreamOperator getOperator() {
    return operator;
  }

  public int getDynamicDivisionNum() {
    return dynamicDivisionNum;
  }

  public void setOperator(StreamOperator operator) {
    this.operator = operator;
  }

  public VertexType getVertexType() {
    return vertexType;
  }

  public Language getLanguage() {
    return language;
  }

  public Map<String, String> getConfig() {
    return config;
  }

  public void setConfig(Map<String, String> config) {
    this.config = config;
  }

  public String getName() {
    if (this.operator != null) {
      if (operator instanceof ChainedOperator || operator instanceof PythonOperator) {
        return String.format("%s-%s", vertexId, operator.getName().split("\n")[0]).trim();
      }
      return String.format("%s-%s", vertexId, operator.getClass().getSimpleName().split("\n")[0])
          .trim();
    }
    return String.format("%s-%s", vertexId, StreamOperator.DEFAULT_NAME).trim();
  }

  public Map<String, Double> getResources() {
    return this.operator.getResource();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("vertexId", vertexId)
        .add("parallelism", parallelism)
        .add("vertexType", vertexType)
        .add("language", language)
        .add("streamOperator", operator)
        .add("config", config)
        .toString();
  }
}
