package io.ray.streaming.runtime.master.scheduler;

import io.ray.streaming.runtime.master.joblifecycle.JobStatus;

/** Job scheduler is used to do the scheduling in JobMaster. */
public interface JobScheduler {

  /**
   * Do the preparatory work for job starting.
   *
   * @return result
   */
  ScheduleResult prepareJobSubmission();

  /**
   * Let the job to start.
   *
   * @return result
   */
  ScheduleResult doJobSubmission();

  /**
   * Destroy all workers created by the job.
   *
   * @param jobStatus specified job status
   * @return true if destroying job succeeded
   */
  boolean destroyJob(JobStatus jobStatus);
}
