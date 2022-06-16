package io.ray.streaming.state.memory;

import io.ray.state.config.MemoryConfig;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.aeonbits.owner.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;

/**
 *  - - - - RayState Memory - - - -
 *     ------------On Heap------------
 *       ———————————————————————————
 *      │      JVM Heap Memory      │
 *      │     (Memory backend/      │
 *      │      off-heap cache)      │
 *       ———————————————————————————
 *     --------------------------------
 *
 *     ------------Off Heap------------
 *       —————————————————————————————
 *      │      JVM Direct Memory      │
 *      │ (Serializer/off-heap cache) │
 *       —————————————————————————————
 *       —————————————————————————————
 *      │        Native memory        │
 *      │      (RocksDB backend)      │
 *       —————————————————————————————
 *     --------------------------------
 *
 *  RayState memory management includes on-heap memory and off-heap memory.
 *
 *  On-heap memory is mainly used for memory backend and on-heap cache.
 *
 *  Off-heap memory is divided into JVM Direct Memory  controlled by JVM and
 *  Native memory not controlled by JVM.
 *  Direct memory is mainly used for IO operations(Serialization) and off-heap cache.
 *  Native memory is mainly third-party plugins, such as RocksDB.
 */
public class MemoryManager {

  private static final Logger LOG = LoggerFactory.getLogger(MemoryManager.class);

  private final long totalMemorySize;
  private final long heapMemorySize;
  private final long offHeapMemorySize;

  private long usedMemorySize;

  private final int segmentSize;

  private final Map<Object, Set<MemorySegment>> allocatedSegments;
  private final List<NativeMemoryInfo> allocatedNativeMemoryInfo;

  private final Object lock = new Object();

  public MemoryManager(Map<String, String> config) {

    MemoryConfig memoryConfig = ConfigFactory.create(MemoryConfig.class, config);

    this.totalMemorySize = ((long) memoryConfig.stateTotalMemoryMb()) * 1024 * 1024;

    final double heapSizeRatio = memoryConfig.onHeapMemorySizeRatio();
    final double offHeapSizeRatio = memoryConfig.offHeapMemorySizeRatio();
    checkArgument((heapSizeRatio + offHeapSizeRatio) <= 1,
        "The ratio of on-heap and the ratio of off-heap is less than 1.");
    this.heapMemorySize = (long) (totalMemorySize * heapSizeRatio);
    this.offHeapMemorySize = (long) (totalMemorySize * offHeapSizeRatio);

    this.segmentSize = memoryConfig.defaultSegmentSize();
    this.allocatedSegments = new ConcurrentHashMap<>();
    this.allocatedNativeMemoryInfo = new CopyOnWriteArrayList<>();

    LOG.info("Create MemoryManager success, RayState total memory size:{}, on-heap size ratio:{}, " +
            "on-heap size: {}, off-heap size ratio:{}, off-heap size:{}, segment size:{}.",
        totalMemorySize, heapSizeRatio, heapMemorySize, offHeapSizeRatio, offHeapMemorySize, segmentSize);
  }

  public boolean registerNativeMemory(NativeMemoryInfo ownerInfo) {
    synchronized (lock) {
      LOG.info("Register native memory, memory info:{}.", ownerInfo.toString());
      long requestMemorySize = ownerInfo.getTotalMemorySize();
      if (requestMemorySize > totalMemorySize) {
        LOG.warn("Register native memory failed, total memory size: {}, available memory size:{}, " +
            "register memory size:{}.", totalMemorySize, getAvailableMemorySize(), requestMemorySize);
        return false;
      } else {
        allocatedNativeMemoryInfo.add(ownerInfo);
        usedMemorySize += requestMemorySize;
      }
    }
    LOG.info("Register native memory success.");
    return true;
  }

  private long getAvailableMemorySize() {
    return totalMemorySize - usedMemorySize;
  }

  public long getTotalMemorySize() {
    return totalMemorySize;
  }
}
