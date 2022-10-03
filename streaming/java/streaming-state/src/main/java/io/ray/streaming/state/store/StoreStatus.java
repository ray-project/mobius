package io.ray.streaming.state.store;

/** Store status for detail description. */
public class StoreStatus {
  private final Long capacity;
  private final Long fileCount;
  private final Long directoryCount;

  public StoreStatus(Long capacity, Long fileCount, Long directoryCount) {
    this.capacity = capacity;
    this.fileCount = fileCount;
    this.directoryCount = directoryCount;
  }

  public Long getCapacity() {
    return capacity;
  }

  public Long getFileCount() {
    return fileCount;
  }

  public Long getDirectoryCount() {
    return directoryCount;
  }
}
