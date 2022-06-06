package io.ray.streaming.runtime.core.processor;

import io.ray.streaming.api.collector.Collector;
import io.ray.streaming.api.context.RuntimeContext;
import io.ray.streaming.operator.StreamOperator;
import java.io.Serializable;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * StreamingProcessor is a process unit for a operator.
 */
public abstract class StreamProcessor<O extends StreamOperator> implements Processor<O> {

  private static final Logger LOG = LoggerFactory.getLogger(StreamProcessor.class);

  protected List<Collector> collectors;
  protected RuntimeContext runtimeContext;
  protected O operator;

  protected volatile boolean isLoadingCheckpoint = false;

  public StreamProcessor(O operator) {
    this.operator = operator;
  }

  @Override
  public void open(List<Collector> collectors, RuntimeContext runtimeContext) {
    this.collectors = collectors;
    this.runtimeContext = runtimeContext;
    if (operator != null) {
      this.operator.open(collectors, runtimeContext);
    }
    LOG.info("opened {}", this);
  }

  @Override
  public void close() {
    operator.close();
  }

  @Override
  public Serializable saveCheckpoint(long checkpointId){
    // for now, there's no StateManager to maintain the cp id
    throw new UnsupportedOperationException();
//    return operator.saveCheckpoint(checkpointId);
  }

  @Override
  public void loadCheckpoint(long checkpointId) throws Exception {
    LOG.info("Do processor state rollback: {}.", checkpointId);
    isLoadingCheckpoint = true;
    try {
      operator.loadCheckpoint(checkpointId);
    } catch (Exception e) {
      clearLoadingCheckpoint();
      throw new RuntimeException("Load checkpoint failed.", e);
    }
    clearLoadingCheckpoint();
  }

  @Override
  public boolean isLoadingCheckpoint() {
    return isLoadingCheckpoint;
  }

  @Override
  public void clearLoadingCheckpoint() {
    isLoadingCheckpoint = false;
  }


  @Override
  public void finish(long checkpointId) throws Exception {
    operator.finish(checkpointId);
    LOG.info("{} finished, checkpoint id: {}.", operator.getClass().getSimpleName(), checkpointId);
  }

  @Override
  public O getOperator() {
    return operator;
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName();
  }
}
