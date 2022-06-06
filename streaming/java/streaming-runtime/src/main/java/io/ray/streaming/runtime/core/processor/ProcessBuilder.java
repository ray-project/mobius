package io.ray.streaming.runtime.core.processor;

import io.ray.streaming.common.enums.OperatorInputType;
import io.ray.streaming.operator.OneInputOperator;
import io.ray.streaming.operator.ISourceOperator;
import io.ray.streaming.operator.StreamOperator;
import io.ray.streaming.operator.TwoInputOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessBuilder {

  private static final Logger LOGGER = LoggerFactory.getLogger(ProcessBuilder.class);

  public static StreamProcessor buildProcessor(StreamOperator streamOperator) {
    OperatorInputType type = streamOperator.getOpType();
    LOGGER.info(
        "Building StreamProcessor, operator type = {}, operator = {}.",
        type,
        streamOperator.getClass().getSimpleName());
    switch (type) {
      case SOURCE:
        return new SourceProcessor((ISourceOperator) streamOperator);
      case ONE_INPUT:
        return new OneInputProcessor((OneInputOperator) streamOperator);
      case TWO_INPUT:
        return new TwoInputProcessor((TwoInputOperator) streamOperator);
      default:
        throw new RuntimeException("current operator type is not support");
    }
  }
}
