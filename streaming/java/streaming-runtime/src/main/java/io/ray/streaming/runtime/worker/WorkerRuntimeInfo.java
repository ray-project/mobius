package io.ray.streaming.runtime.worker;

import com.google.common.base.MoreObjects;
import io.ray.streaming.common.utils.EnvUtil;
import java.io.Serializable;
import org.apache.commons.lang3.StringUtils;

/**
 * Basic process info of job worker.
 */
public class WorkerRuntimeInfo implements Serializable {

  /**
   * Is worker healthy.
   */
  private boolean healthy;

  /**
   * Cluster name.
   */
  private String clusterName;

  /**
   * Datecenter name.
   */
  private String idcName;

  /**
   * Previous process id.
   */
  private String previousPid;

  /**
   * Process id.
   */
  private String pid;

  /**
   * Previous node's hostname which the process is located.
   */
  private String previousHostname;

  /**
   * Node's hostname which the process is located.
   */
  private String hostname;

  /**
   * Previous node's ip address which the process is located.
   */
  private String previousIpAddress;

  /**
   * Node's ip address which the process is located.
   */
  private String ipAddress;

  public WorkerRuntimeInfo() {
    this(false);
  }

  public WorkerRuntimeInfo(boolean useCurrentEnv) {
    if (useCurrentEnv) {
      clusterName = EnvUtil.getClusterName();
      idcName = EnvUtil.getIDCName();
      pid = EnvUtil.getJvmPid();
      previousPid = pid;
      hostname = EnvUtil.getHostName();
      previousHostname = hostname;
      ipAddress = EnvUtil.getHostAddress();
      previousIpAddress = ipAddress;
    }
  }

  public String getClusterName() {
    return clusterName;
  }

  public void setClusterName(String clusterName) {
    this.clusterName = clusterName;
  }

  public String getIdcName() {
    return idcName;
  }

  public void setIdcName(String idcName) {
    this.idcName = idcName;
  }

  public String getPreviousPid() {
    return previousPid;
  }

  public String getPid() {
    return pid;
  }

  public void setPid(String pid) {
    this.pid = pid;
  }

  public String getPreviousHostname() {
    return previousHostname;
  }

  public String getHostname() {
    return hostname;
  }

  public void setHostname(String hostname) {
    this.hostname = hostname;
  }

  public String getPreviousIpAddress() {
    return previousIpAddress;
  }

  public String getIpAddress() {
    return ipAddress;
  }

  public void setIpAddress(String ipAddress) {
    this.ipAddress = ipAddress;
  }

  public boolean isHealthy() {
    return healthy;
  }

  public void setHealthy(boolean healthy) {
    this.healthy = healthy;
  }

  public void update(WorkerRuntimeInfo workerRuntimeInfo) {
    if (workerRuntimeInfo == null) {
      return;
    }

    healthy = workerRuntimeInfo.isHealthy();
    clusterName = workerRuntimeInfo.getClusterName();
    idcName = workerRuntimeInfo.getIdcName();

    if (StringUtils.isEmpty(previousPid)) {
      previousPid = workerRuntimeInfo.pid;
      pid = previousPid;
    } else {
      previousPid = pid;
      pid = workerRuntimeInfo.pid;
    }

    if (StringUtils.isEmpty(previousHostname)) {
      previousHostname = workerRuntimeInfo.hostname;
      hostname = previousHostname;
    } else {
      previousHostname = hostname;
      hostname = workerRuntimeInfo.hostname;
    }

    if (StringUtils.isEmpty(previousIpAddress)) {
      previousIpAddress = workerRuntimeInfo.ipAddress;
      ipAddress = previousIpAddress;
    } else {
      previousIpAddress = ipAddress;
      ipAddress = workerRuntimeInfo.ipAddress;
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("healthy", healthy)
        .add("clusterName", clusterName)
        .add("idcName", idcName)
        .add("previousPid", previousPid)
        .add("pid", pid)
        .add("previousHostname", previousHostname)
        .add("hostname", hostname)
        .add("previousIpAddress", previousIpAddress)
        .add("ipAddress", ipAddress)
        .toString();
  }
}
