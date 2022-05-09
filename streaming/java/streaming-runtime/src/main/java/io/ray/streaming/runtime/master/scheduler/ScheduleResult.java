package io.ray.streaming.runtime.master.scheduler;

import io.ray.sreaming.common.utils.CommonUtil;

/**
 * To unify return info for scheduling event like 'job-submission'.
 */
public class ScheduleResult {

  private static final String NO_EXCEPT = "NO_EXCEPTION";

  private boolean result;

  private ScheduleException scheduleException;

  public ScheduleResult(boolean result) {
    this(result, null);
  }

  public ScheduleResult(boolean result, ScheduleException scheduleException) {
    this.result = result;
    this.scheduleException = scheduleException;
  }

  public static ScheduleResult success() {
    return new ScheduleResult(true);
  }

  public static ScheduleResult fail() {
    return new ScheduleResult(false);
  }

  public static ScheduleResult fail(String failMsg) {
    return new ScheduleResult(false, new ScheduleException(failMsg));
  }

  public static ScheduleResult fail(ScheduleException scheduleException) {
    return new ScheduleResult(false, scheduleException);
  }

  public boolean result() {
    return result;
  }

  public String getExceptionMsg() {
    if (scheduleException != null) {
      return scheduleException.getMessage();
    }
    return NO_EXCEPT;
  }

  public String getFullExceptionMsg() {
    if (scheduleException != null) {
      return CommonUtil.getStackMsg(scheduleException);
    }
    return NO_EXCEPT;
  }
}
