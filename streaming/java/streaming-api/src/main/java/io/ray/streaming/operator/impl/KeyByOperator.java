package io.ray.streaming.operator.impl;

import io.ray.streaming.api.function.impl.KeyFunction;
import io.ray.streaming.message.KeyRecord;
import io.ray.streaming.message.Record;
import io.ray.streaming.operator.AbstractStreamOperator;
import io.ray.streaming.operator.OneInputOperator;
import io.ray.streaming.operator.StreamOperator;
import io.ray.streaming.util.TypeInfo;

public class KeyByOperator<T, K> extends AbstractStreamOperator<KeyFunction<T, K>>
    implements OneInputOperator<T> {

  public KeyByOperator(KeyFunction<T, K> keyFunction) {
    super(keyFunction);
  }

  @Override
  public void processElement(Record<T> record) throws Exception {
    K key = this.function.keyBy(record.getValue());
    collect(new KeyRecord<>(key, record.getValue()));
  }

  @SuppressWarnings("unchecked")
  @Override
  public TypeInfo<T> getInputTypeInfo() {
    return typeInfo;
  }
}
