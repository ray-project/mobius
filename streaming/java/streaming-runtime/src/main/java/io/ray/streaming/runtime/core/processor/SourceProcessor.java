package io.ray.streaming.runtime.core.processor;

import io.ray.streaming.message.Record;
import io.ray.streaming.operator.ISourceOperator;

/** The processor for the stream sources, containing a SourceOperator. */
public class SourceProcessor extends StreamProcessor<ISourceOperator> {

  public SourceProcessor(ISourceOperator operator) {
    super(operator);
  }

  @Override
  public void process(Record record) {
    operator.fetch(runtimeContext.getCheckpointId());
  }
}
