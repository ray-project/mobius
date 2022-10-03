package io.ray.streaming.runtime.core.processor;

import io.ray.streaming.message.Record;
import io.ray.streaming.operator.TwoInputOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TwoInputProcessor extends StreamProcessor<TwoInputOperator> {

  private static final Logger LOGGER = LoggerFactory.getLogger(TwoInputProcessor.class);

  private String leftStream;
  private String rightStream;

  public TwoInputProcessor(TwoInputOperator operator) {
    super(operator);
  }

  @Override
  public void process(Record record) {
    String streamName = record.getStream();
    if (leftStream.equals(streamName)) {
      try {
        operator.processElement(record, null);
      } catch (Exception e) {
        LOGGER.error("TwoInputProcessor processes left stream failed", e);
        throw new RuntimeException(e);
      }
    } else if (rightStream.equals(streamName)) {
      try {
        operator.processElement(null, record);
      } catch (Exception e) {
        LOGGER.error("TwoInputProcessor processes right stream failed", e);
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void close() {
    this.operator.close();
  }

  public String getLeftStream() {
    return leftStream;
  }

  public void setLeftStream(String leftStream) {
    this.leftStream = leftStream;
  }

  public String getRightStream() {
    return rightStream;
  }

  public void setRightStream(String rightStream) {
    this.rightStream = rightStream;
  }
}
