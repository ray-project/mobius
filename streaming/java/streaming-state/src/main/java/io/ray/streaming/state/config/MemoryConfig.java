package io.ray.streaming.state.config;

/** Used for setup in-cache state. NOTE: This config is nothing to do with the Memory Backend. */
public interface MemoryConfig extends StateConfig {

  String ON_HEAP_MEMORY_SIZE_RATIO = "state.buffer.on-heap.size.ratio";

  String OFF_HEAP_MEMORY_SIZE_RATIO = "state.buffer.off-heap.size.ratio";

  String MEMORY_SEGMENT_SIZE = "state.buffer.segment.size";

  String STATE_MEMORY_TOTAL_MB = "state.buffer.total.mb";

  //  String STATE_MEMORY_NATIVE_ROCKSDB_MB = "state.buffer.native.rocksdb.mb";
  //  String STATE_MEMORY_ROCKSDB_WRITE_BUFFER_RATIO = "state.buffer.rocksdb.write-buffer.ratio";
  //  String STATE_MEMORY_ROCKSDB_HIGH_PRIORITY_POOL_RATIO =
  // "state.buffer.rocksdb.high-priority-pool.ratio";

  @DefaultValue("0.2")
  @Key(value = ON_HEAP_MEMORY_SIZE_RATIO)
  double onHeapMemorySizeRatio();

  @DefaultValue("0.8")
  @Key(value = OFF_HEAP_MEMORY_SIZE_RATIO)
  double offHeapMemorySizeRatio();

  /** Memory segment size, default 32K. */
  @DefaultValue("32768")
  @Key(value = MEMORY_SEGMENT_SIZE)
  int defaultSegmentSize();

  @DefaultValue("1536")
  @Key(value = STATE_MEMORY_TOTAL_MB)
  int stateTotalMemoryMb();

  // -------------------------------------------------------------------------------
  //  RocksDB buffer config
  // -------------------------------------------------------------------------------
  //  /**
  //   * The ratio of total buffer is 90%.
  //   * @return
  //   */
  //  @DefaultValue("972")
  //  @Key(value = STATE_MEMORY_NATIVE_ROCKSDB_MB)
  //  long rocksDBMemory();
  //
  //  /**
  //   * Write buffer ratio of rocksdb total buffer is 70%.
  //   */
  //  @DefaultValue("0.7")
  //  @Key(value = STATE_MEMORY_ROCKSDB_WRITE_BUFFER_RATIO)
  //  double writeBufferRatio();
  //
  //  /**
  //   * High priority pool ratio of rocksdb total buffer is 10%.
  //   */
  //  @DefaultValue("0.1")
  //  @Key(value = STATE_MEMORY_ROCKSDB_HIGH_PRIORITY_POOL_RATIO)
  //  double highPriorityPoolRatio();

}
