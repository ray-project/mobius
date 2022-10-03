package io.ray.streaming.runtime.util;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.sun.management.OperatingSystemMXBean;
import io.ray.streaming.common.enums.ResourceKey;
import io.ray.streaming.runtime.core.resource.Container;
import io.ray.streaming.runtime.core.resource.ContainerId;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Resource Utility collects current OS and JVM resource usage information */
public class ResourceUtil {

  public static final Logger LOG = LoggerFactory.getLogger(ResourceUtil.class);

  // limit by ray, the mem size must be a positive multiple of 50
  public static final int MEMORY_UNIT_SIZE = 50;
  public static final int MEMORY_MB_MIN_VALUE = 128;

  /**
   * Refer to:
   * https://docs.oracle.com/javase/8/docs/jre/api/management/extension/com/sun/management/OperatingSystemMXBean.html
   */
  private static OperatingSystemMXBean osmxb =
      (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

  /** Log current jvm process's buffer detail */
  public static void logProcessMemoryDetail() {
    int mb = 1024 * 1024;

    // Getting the runtime reference from system
    Runtime runtime = Runtime.getRuntime();

    StringBuilder sb = new StringBuilder(32);

    sb.append("used buffer: ")
        .append((runtime.totalMemory() - runtime.freeMemory()) / mb)
        .append(", free buffer: ")
        .append(runtime.freeMemory() / mb)
        .append(", total buffer: ")
        .append(runtime.totalMemory() / mb)
        .append(", max buffer: ")
        .append(runtime.maxMemory() / mb);

    if (LOG.isInfoEnabled()) {
      LOG.info(sb.toString());
    }
  }

  /**
   * @return jvm heap usage ratio. note that one of the survivor space is not include in total
   *     buffer while calculating this ratio.
   */
  public static double getJvmHeapUsageRatio() {
    Runtime runtime = Runtime.getRuntime();
    return (runtime.totalMemory() - runtime.freeMemory()) * 1.0 / runtime.maxMemory();
  }

  /**
   * @return jvm heap usage(in bytes). note that this value doesn't include one of the survivor
   *     space.
   */
  public static long getJvmHeapUsageInBytes() {
    Runtime runtime = Runtime.getRuntime();
    return runtime.totalMemory() - runtime.freeMemory();
  }

  /** Returns the total amount of physical buffer in bytes. */
  public static long getSystemTotalMemory() {
    return osmxb.getTotalPhysicalMemorySize();
  }

  /** Returns the used system physical buffer in bytes */
  public static long getSystemMemoryUsage() {
    long totalMemory = osmxb.getTotalPhysicalMemorySize();
    long freeMemory = osmxb.getFreePhysicalMemorySize();
    return totalMemory - freeMemory;
  }

  /** Returns the ratio of used system physical buffer. This value is a double in the [0.0,1.0] */
  public static double getSystemMemoryUsageRatio() {
    double totalMemory = osmxb.getTotalPhysicalMemorySize();
    double freeMemory = osmxb.getFreePhysicalMemorySize();
    double ratio = freeMemory / totalMemory;
    return 1 - ratio;
  }

  /** Returns the cpu load for current jvm process. This value is a double in the [0.0,1.0] */
  public static double getProcessCpuUsage() {
    return osmxb.getProcessCpuLoad();
  }

  /**
   * @return the system cpu usage. This value is a double in the [0.0,1.0] We will try to use `vsar`
   *     to get cpu usage by default, and use MXBean if any exception raised.
   */
  public static double getSystemCpuUsage() {
    double cpuUsage = 0.0;
    try {
      cpuUsage = getSystemCpuUtilByVsar();
    } catch (Exception e) {
      cpuUsage = getSystemCpuUtilByMXBean();
    }
    return cpuUsage;
  }

  /**
   * @return the "recent cpu usage" for the whole system. This value is a double in the [0.0,1.0]
   *     interval. A value of 0.0 means that all CPUs were idle during the recent period of time
   *     observed, while a value of 1.0 means that all CPUs were actively running 100% of the time
   *     during the recent period being observed
   */
  public static double getSystemCpuUtilByMXBean() {
    return osmxb.getSystemCpuLoad();
  }

  /** Get system cpu util by vsar */
  public static double getSystemCpuUtilByVsar() throws Exception {
    double cpuUsageFromVsar = 0.0;
    String[] vsarCpuCommand = {"/bin/sh", "-c", "vsar --check --cpu -s util"};
    try {
      Process proc = Runtime.getRuntime().exec(vsarCpuCommand);
      BufferedInputStream bis = new BufferedInputStream(proc.getInputStream());
      BufferedReader br = new BufferedReader(new InputStreamReader(bis));
      String line;
      List<String> processPidList = new ArrayList<>();
      while ((line = br.readLine()) != null) {
        processPidList.add(line);
      }
      if (!processPidList.isEmpty()) {
        String[] split = processPidList.get(0).split("=");
        cpuUsageFromVsar = Double.parseDouble(split[1]) / 100.0D;
      } else {
        throw new IOException("Vsar check cpu usage failed, maybe vsar is not installed.");
      }
    } catch (Exception e) {
      LOG.warn("Failed to get cpu usage by vsar.", e);
      throw e;
    }
    return cpuUsageFromVsar;
  }

  /** Returns the system load average for the last minute */
  public static double getSystemLoadAverage() {
    return osmxb.getSystemLoadAverage();
  }

  /** Returns system cpu cores num */
  public static int getCpuCores() {
    return osmxb.getAvailableProcessors();
  }

  /**
   * Get containers by hostname of address
   *
   * @param containers container list
   * @param containerHosts container hostname or address set
   * @return matched containers
   */
  public static List<Container> getContainersByHostname(
      List<Container> containers, Collection<String> containerHosts) {

    return containers.stream()
        .filter(
            container ->
                containerHosts.contains(container.getHostname())
                    || containerHosts.contains(container.getAddress()))
        .collect(Collectors.toList());
  }

  /**
   * Get container by hostname
   *
   * @param hostName container hostname
   * @return container
   */
  public static Optional<Container> getContainerByHostname(
      List<Container> containers, String hostName) {
    return containers.stream()
        .filter(
            container ->
                container.getHostname().equals(hostName) || container.getAddress().equals(hostName))
        .findFirst();
  }

  /**
   * Get container by id
   *
   * @param containerID container id
   * @return container
   */
  public static Optional<Container> getContainerById(
      List<Container> containers, ContainerId containerID) {
    return containers.stream()
        .filter(container -> container.getId().equals(containerID))
        .findFirst();
  }

  /**
   * Transfer the buffer properly to fit the buffer value used for ray internal(divisible by 50).
   *
   * <p>Notice: 1) if the input buffer value < 128, it will be used as gb unit 2) if the input
   * buffer value >= 128, it will be used as mb unit
   *
   * @param memoryValue the buffer value
   * @return the properly value
   */
  public static long getMemoryMbFromMemoryValue(Double memoryValue) {
    if (isMemoryMbValue(memoryValue.longValue())) {
      // for mb value
      if (memoryValue.longValue() % MEMORY_UNIT_SIZE == 0) {
        return memoryValue.longValue();
      }
      return Double.valueOf(
              (memoryValue.longValue() + (MEMORY_UNIT_SIZE - 1))
                  / MEMORY_UNIT_SIZE
                  * MEMORY_UNIT_SIZE)
          .longValue();
    } else {
      // for gb value
      return Double.valueOf(
              (memoryValue.longValue() * 1024 + (MEMORY_UNIT_SIZE - 1))
                  / MEMORY_UNIT_SIZE
                  * MEMORY_UNIT_SIZE)
          .longValue();
    }
  }

  /**
   * Judge whether the value is valid to describe buffer in mb. e.g. if user use gb-value, the value
   * should be like 2,4
   *
   * @param value buffer value
   * @return is a valid to describe buffer in mb
   */
  public static boolean isMemoryMbValue(long value) {
    return value >= MEMORY_MB_MIN_VALUE;
  }

  /**
   * Format cpu value for ray's limitation. limitation: if 0 < cpu <= 1, value can be decimal if cpu
   * > 1, value must be integer
   *
   * @param cpuValue the target value
   * @return formatted value
   */
  public static double formatCpuValue(double cpuValue) {
    if (cpuValue > 1) {
      return Math.round(cpuValue);
    } else if (cpuValue > 0) {
      return cpuValue;
    }
    return 1D;
  }

  /**
   * Format resource map for ray's limitation.
   *
   * @param resource target resource
   * @return formatted resource
   */
  public static Map<String, Double> formatResource(Map<String, Double> resource) {
    // format cpu
    if (resource.containsKey(ResourceKey.CPU.name())) {
      resource.put(ResourceKey.CPU.name(), formatCpuValue(resource.get(ResourceKey.CPU.name())));
    }
    return resource;
  }

  /**
   * Transfer operator resources into map.
   *
   * @param customOpResourceConfig resource config in job config
   * @return map result
   */
  public static Map<String, Map<String, Double>> resolveOperatorResourcesFromJobConfig(
      String customOpResourceConfig) {
    try {
      return new Gson()
          .fromJson(
              customOpResourceConfig,
              new TypeToken<Map<String, Map<String, Double>>>() {}.getType());
    } catch (Exception e) {
      LOG.error("Failed to resolve operator resource from job config: {}.", customOpResourceConfig);
    }
    return Collections.emptyMap();
  }
}
