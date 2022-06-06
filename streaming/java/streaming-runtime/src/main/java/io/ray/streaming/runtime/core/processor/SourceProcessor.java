package io.ray.streaming.runtime.core.processor;

import io.ray.streaming.message.Record;
import io.ray.streaming.operator.ISourceOperator;

/**
 * The processor for the stream sources, containing a ISourceOperator.
 *
 * @param <T> The type of source data.
 */
public class SourceProcessor<T> extends StreamProcessor<Record, ISourceOperator<T>> {

  public SourceProcessor(ISourceOperator<T> operator) {
    super(operator);
  }

  @Override
  public void process(Record record) {
    throw new UnsupportedOperationException("SourceProcessor should not process record");
  }

  public void fetch() {
    operator.fetch();
  }

  @Override
  public void close() {}
}
