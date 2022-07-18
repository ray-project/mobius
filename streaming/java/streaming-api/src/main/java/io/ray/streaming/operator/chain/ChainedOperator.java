package io.ray.streaming.operator.chain;

import com.google.common.base.Preconditions;
import io.ray.streaming.api.Language;
import io.ray.streaming.api.collector.Collector;
import io.ray.streaming.api.context.RuntimeContext;
import io.ray.streaming.api.function.Function;
import io.ray.streaming.common.enums.OperatorInputType;
import io.ray.streaming.common.utils.CommonUtil;
import io.ray.streaming.operator.AbstractStreamOperator;
import io.ray.streaming.operator.OneInputOperator;
import io.ray.streaming.operator.StreamOperator;
import io.ray.streaming.util.OperatorUtil;
import io.ray.streaming.util.TypeInfo;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Abstract base class for chained operators. */
public abstract class ChainedOperator extends AbstractStreamOperator<Function> {
  private static final Logger LOG = LoggerFactory.getLogger(ChainedOperator.class);
  /** Chained operators */
  protected final List<StreamOperator> operators;
  protected final StreamOperator headOperator;
  protected Set<StreamOperator> tailOperators;
  private final List<Map<String, String>> opConfigs;
  private final Map<String, Double> resources;
  private String chainedOperatorName;

  public ChainedOperator(
      List<StreamOperator> operators,
      List<Map<String, String>> opConfigs,
      List<Map<String, Double>> resources) {
    Preconditions.checkArgument(
        operators.size() >= 2, "Need at lease two operators to be chained together");
    operators.stream()
        .skip(1)
        .forEach(operator -> Preconditions.checkArgument(operator instanceof OneInputOperator));
    this.tailOperators = new HashSet<>();
    this.operators = operators;
    // keep multi configs in list
    this.opConfigs = opConfigs;
    // chain into 1 map
    this.opConfig = CommonUtil.chainConfigs(opConfigs);
    this.resources = CommonUtil.chainResources(resources);
    this.headOperator = operators.get(0);
    this.tailOperators = OperatorUtil.generateTailOperators(this.operators);
  }

  @Override
  public void open(List<Collector> collectorList, RuntimeContext runtimeContext) {
    // Dont' call super.open() as we `open` every operator separately.
    LOG.info("chainedOperator open.");
    for (int i = 0; i < operators.size(); i++) {
      StreamOperator operator = operators.get(i);
      List<Collector> succeedingCollectors = new ArrayList<>();
      operator
          .getNextOperators()
          .forEach(
              subOperator -> {
                if (operators.contains(subOperator)) {
                  succeedingCollectors.add(new ForwardCollector((OneInputOperator) subOperator));
                } else {
                  succeedingCollectors.addAll(
                      collectorList.stream()
                          .filter(
                              collector ->
                                  (collector.getId() == operator.getId()
                                      && collector.getDownStreamOpId() == subOperator.getId()))
                          .collect(Collectors.toList()));
                }
              });
      operator.open(succeedingCollectors, createRuntimeContext(runtimeContext, i));
    }
  }

  private RuntimeContext createRuntimeContext(RuntimeContext runtimeContext, int index) {
    return (RuntimeContext)
        Proxy.newProxyInstance(
            runtimeContext.getClass().getClassLoader(),
            new Class[] {RuntimeContext.class},
            (proxy, method, methodArgs) -> {
              if (method.getName().equals("getOpConfig")) {
                return opConfigs.get(index);
              } else {
                return method.invoke(runtimeContext, methodArgs);
              }
            });
  }

  /**
   * To mark operator has been finished within this checkpoint, which indicate all of sync-like
   * actions will be done in this function in orderless.
   *
   * @param checkpointId
   * @throws Exception
   */
  @Override
  public void finish(long checkpointId) throws Exception {
    for (int i = 0; i < operators.size(); ++i) {
      operators.get(i).finish(checkpointId);
    }
  }

  @Override
  public void saveCheckpoint(long checkpointId) throws Exception {
    for (int i = 0; i < operators.size(); ++i) {
      operators.get(i).saveCheckpoint(checkpointId);
    }
  }

  @Override
  public void loadCheckpoint(long checkpointId) throws Exception {
    for (int i = 0; i < operators.size(); ++i) {
      operators.get(i).loadCheckpoint(checkpointId);
    }
  }

  @Override
  public void deleteCheckpoint(long checkpointId) throws Exception {
    for (int i = 0; i < operators.size(); ++i) {
      operators.get(i).deleteCheckpoint(checkpointId);
    }
  }

  @Override
  public void close() {
    operators.forEach(StreamOperator::close);
  }

  @Override
  public OperatorInputType getOpType() {
    return headOperator.getOpType();
  }

  @Override
  public Language getLanguage() {
    return headOperator.getLanguage();
  }

  @Override
  public String getName() {
    return operators.get(0).getClass().getSimpleName();
  }

  public String getChainedOperatorName() {
    if (this.chainedOperatorName == null) {
      StringBuilder name = new StringBuilder();
      OperatorUtil.createName(operators, operators.get(0), name);
      this.chainedOperatorName = name.toString();
    }
    return this.chainedOperatorName;
  }

  public List<StreamOperator> getOperators() {
    return operators;
  }

  @Override
  public boolean hasTypeInfo() {
    if (tailOperators.size() > 1) {
      throw new UnsupportedOperationException(
          "This chainedOperator has at least two tail operators, can't call 'hasTypeInfo'");
    }
    return tailOperators.iterator().next().hasTypeInfo();
  }

  @Override
  public TypeInfo getTypeInfo() {
    if (tailOperators.size() > 1) {
      throw new UnsupportedOperationException(
          "This chainedOperator has at least two tail operators, can't call 'getTypeInfo'");
    }
    return tailOperators.iterator().next().getTypeInfo();
  }

  @Override
  public boolean hasSchema() {
    if (tailOperators.size() > 1) {
      throw new UnsupportedOperationException(
          "This chainedOperator has at least two tail operators, can't call 'hasSchema'");
    }
    return tailOperators.iterator().next().hasSchema();
  }

  @Override
  public Schema getSchema() {
    if (tailOperators.size() > 1) {
      throw new UnsupportedOperationException(
          "This chainedOperator has at least two tail operators, can't call 'getSchema'");
    }
    return tailOperators.iterator().next().getSchema();
  }

  @Override
  public void setSchema(Schema schema) {
    if (tailOperators.size() > 1) {
      throw new UnsupportedOperationException(
          "This chainedOperator has at least two tail operators, can't call 'setSchema'");
    }
    tailOperators.iterator().next().setSchema(schema);
  }

  @Override
  public Set<StreamOperator> getTailOperatorSet() {
    return tailOperators;
  }

  @Override
  public Map<String, Double> getResource() {
    return resources;
  }

  public List<Map<String, String>> getOpConfigs() {
    return opConfigs;
  }

  @Override
  public List<StreamOperator> getNextOperators() {
    return Collections.singletonList(headOperator);
  }

  @Override
  public int getId() {
    return headOperator.getId();
  }

  @Override
  public void forwardCommand(String commandMessage) {
    for (int i = 0; i < operators.size(); ++i) {
      operators.get(i).forwardCommand(commandMessage);
    }
  }
}
