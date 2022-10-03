package io.ray.streaming.runtime.core.processor;

import io.ray.streaming.api.collector.Collector;
import io.ray.streaming.api.context.RuntimeContext;
import io.ray.streaming.api.function.Function;
import io.ray.streaming.message.Record;
import io.ray.streaming.operator.StreamOperator;
import java.io.Serializable;
import java.util.List;

public interface Processor<O extends StreamOperator> extends Serializable {

  void open(List<Collector> collectors, RuntimeContext runtimeContext);

  void process(Record record);

  void finish(long checkpointId) throws Exception;

  void close();

  /** See {@link Function#saveCheckpoint()}. */
  Serializable saveCheckpoint(long checkpointId);

  /** See {@link Function#loadCheckpoint(Serializable)}. */
  void loadCheckpoint(long checkpointId) throws Exception;

  default void closeState() {
    throw new UnsupportedOperationException();
  }

  default boolean isLoadingCheckpoint() {
    return false;
  }

  void clearLoadingCheckpoint();

  O getOperator();
}
