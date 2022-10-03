package io.ray.streaming.runtime.master.joblifecycle;

/** Kee this in sync with `ray.streaming.session.JobStatus` */
public enum JobStatus {

  // The initial state of a job.
  SUBMITTING,

  // Job has been submitted and is long running.
  RUNNING,

  // Job is finished, clear worker by killing.
  FINISHED,

  // Job is finished, clear worker by calling.
  FINISHED_AND_CLEAN,

  // Job submission failed: passive cancel
  SUBMITTING_FAILED,

  // Job submission cancelled: active cancel
  SUBMITTING_CANCELLED,

  // Job wait for resubmit.
  RESUBMITTING,

  // Unknown state only when trying to transfer an error string to JobStatus.
  UNKNOWN;

  public static JobStatus toStatus(String status) {
    if (null == status) {
      return UNKNOWN;
    }
    for (JobStatus value : JobStatus.values()) {
      if (value.name().equalsIgnoreCase(status)) {
        return value;
      }
    }
    return UNKNOWN;
  }
}
