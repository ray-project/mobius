package io.ray.streaming.operator.impl;

import io.ray.streaming.api.function.impl.SinkFunction;
import io.ray.streaming.message.Record;
import io.ray.streaming.operator.AbstractStreamOperator;
import io.ray.streaming.operator.OneInputOperator;
import io.ray.streaming.operator.StreamOperator;
import io.ray.streaming.util.TypeInfo;

public class SinkOperator<T> extends AbstractStreamOperator<SinkFunction<T>>
    implements OneInputOperator<T> {

  public SinkOperator(SinkFunction<T> sinkFunction) {
    super(sinkFunction);
  }

  @Override
  public void processElement(Record<T> record) throws Exception {
    this.function.sink(record.getValue());
  }

  @SuppressWarnings("unchecked")
  @Override
  public TypeInfo<T> getInputTypeInfo() {
    return typeInfo;
  }
}
