package io.ray.streaming.runtime.master.scheduler;

/** To find out if worker or independent actor support health check. */
public interface HealthCheckable {

  boolean healthCheckable();
}
