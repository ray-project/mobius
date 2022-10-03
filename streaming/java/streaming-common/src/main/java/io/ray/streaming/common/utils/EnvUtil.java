package io.ray.streaming.common.utils;

import com.google.common.collect.Sets;
import com.sun.jna.NativeLibrary;
import io.ray.runtime.util.BinaryFileUtil;
import io.ray.runtime.util.JniUtils;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EnvUtil {

  private static final Logger LOG = LoggerFactory.getLogger(EnvUtil.class);

  private static final String CLUSTER_NAME_ENV_KEY = "cluster_name";
  private static final String NAMESPACE_ENV_KEY = "NAMESPACE";
  private static Set<String> loadedLibs = Sets.newHashSet();
  private static final String IDC_NAME_ENV_KEY = "IDCNAME";
  private static final String JOB_DIR_ENV_KEY = "RAY_JOB_DIR";

  public static String getClusterName() {
    String clusterName = System.getenv(CLUSTER_NAME_ENV_KEY);
    if (StringUtils.isEmpty(clusterName)) {
      LOG.debug("{} env variable is empty.", CLUSTER_NAME_ENV_KEY);
      return "";
    }
    return clusterName;
  }

  public static String getNamespace() {
    String namespace = System.getenv(NAMESPACE_ENV_KEY);
    if (StringUtils.isEmpty(namespace)) {
      LOG.debug("{} env variable is emtpy.", NAMESPACE_ENV_KEY);
    }
    return namespace;
  }

  public static String getHostAddress() {
    String ip = "";
    try {
      ip = InetAddress.getLocalHost().getHostAddress();
    } catch (UnknownHostException e) {
      LOG.error("Error occurs while fetching local ip.", e);
    }
    return ip;
  }

  public static String getHostName() {
    String hostname = "";
    try {
      hostname = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      LOG.error("Error occurs while fetching local ip.", e);
    }
    return hostname;
  }

  public static String getHostnameByAddress(String ip) {
    byte[] address = toIpByte(ip);
    InetAddress addr = null;
    try {
      addr = InetAddress.getByAddress(address);
    } catch (UnknownHostException e) {
      LOG.warn("Unknow hostname for ip : {}", ip);
      return "Unknow";
    }
    return addr.getHostName();
  }

  private static byte[] toIpByte(String ip) {
    String[] ipBytes = ip.split("\\.");
    byte[] address = new byte[ipBytes.length];
    for (int i = 0; i < ipBytes.length; i++) {
      address[i] = (byte) Integer.parseInt(ipBytes[i]);
    }
    return address;
  }

  public static String getJvmPid() {
    return ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
  }

  public static String shellExecute(String cmd, boolean isDaemon) {
    String output = null;
    try {
      LOG.info("Execution command : {}.", cmd);
      String[] cmdArgs = {"sh", "-c", cmd};
      Process process = Runtime.getRuntime().exec(cmdArgs);
      BufferedReader b = new BufferedReader(new InputStreamReader(process.getInputStream()));
      BufferedReader eb = new BufferedReader(new InputStreamReader(process.getErrorStream()));
      String r;
      if (!isDaemon) {
        while ((r = b.readLine()) != null) {
          if (null == output) {
            output = "";
          }
          output += r;
        }
      }
      process.waitFor(3L, TimeUnit.SECONDS);
      if (process.isAlive()) {
        LOG.warn("Destroy process is alive.");
        process.destroyForcibly();
      } else {
        LOG.info("Command is finished.");
      }
      if (0 != process.exitValue()) {
        LOG.error("Exit code {},  error info : {}.", process.exitValue(), eb.readLine());
      }
      b.close();
      eb.close();
    } catch (Exception e) {
      LOG.error("Shell execute exception.", e);
    }
    return output;
  }

  public static String shellExecute(String cmd) {
    return shellExecute(cmd, false);
  }

  public static String getParentPid(String pid) {
    try {
      String ppid =
          shellExecute(String.format("cat /proc/%s/status | grep PPid | awk '{print $2}' ", pid));
      if (null != ppid && !ppid.isEmpty() && !ppid.trim().equals("0") && !ppid.trim().equals("1")) {
        return ppid.trim();
      } else {
        LOG.warn("Ppid is invalid : {}.", ppid);
      }
    } catch (Exception e) {
      LOG.error("Can not get parent pid.", e);
    }
    return null;
  }

  public static String getParentPid() {
    return getParentPid(getJvmPid());
  }

  public static boolean isPortInUsing(String host, int port) {
    InetAddress address;
    try {
      address = InetAddress.getByName(host);
    } catch (UnknownHostException e) {
      LOG.error("Caught exception when get address by name.", e);
      return false;
    }
    try {

      new Socket(address, port);
      return true;
    } catch (IOException e) {
      LOG.warn("Failed to create socket connection with ");
    }
    return false;
  }

  public static String getLocalListenPort() {
    String pid = getJvmPid();
    String result =
        shellExecute(
            "netstat -pan | grep "
                + pid
                + " | grep LISTEN | head -n 1 | "
                + "awk '{print $4}' | cut -d':' -f 2");
    LOG.info("Local listen port : {}.", result);
    try {
      if (null == result || result.trim().isEmpty()) {
        return null;
      }
    } catch (Exception e) {
      LOG.error("Find local listen port exception", e);
    }
    return result;
  }

  public static Map<String, String> getSystemProperties() {
    return ManagementFactory.getRuntimeMXBean().getSystemProperties();
  }

  public static void loadNativeLibraries() {
    JniUtils.loadLibrary(BinaryFileUtil.CORE_WORKER_JAVA_LIBRARY, true);
    io.ray.streaming.common.utils.JniUtils.loadLibrary("streaming_java");
  }

  public static String getWorkingDir() {
    String workingDir = System.getenv(JOB_DIR_ENV_KEY);
    // If no such RAY_JOB_DIR env, we return current directory path.
    if (null == workingDir) {
      workingDir = ".";
    }
    return workingDir;
  }

  /**
   * Loads the native library specified by the <code>libraryName</code> argument. The <code>
   * libraryName </code> argument must not contain any platform specific prefix, file extension or
   * path.
   *
   * @param libraryName the name of the library.
   */
  public static synchronized void loadLibrary(String libraryName, boolean exportSymbols) {
    if (!loadedLibs.contains(libraryName)) {
      LOG.info("Loading native library {}.", libraryName);
      // Load native library.
      String fileName = System.mapLibraryName(libraryName);
      final File file = BinaryFileUtil.getNativeFile(getWorkingDir(), fileName);

      LOG.info("Get native file {}.", fileName);
      if (exportSymbols) {
        // Expose library symbols using RTLD_GLOBAL which may be depended by other shared
        // libraries.
        NativeLibrary.getInstance(file.getAbsolutePath());
      }
      LOG.info("System load file {}.", file.getAbsolutePath());
      // This will bind jni libraries to classloader of caller class, i.e. EnvUtil
      System.load(file.getAbsolutePath());
      LOG.info("Native library {} of {} loaded.", libraryName, file.getAbsolutePath());
      loadedLibs.add(libraryName);
    }
  }

  /** Exit process. */
  public static void shutdown() {
    if (!TestHelper.isUTPattern()) {
      System.exit(0);
    }
  }

  /**
   * Exit process by specified pid.
   *
   * @param pid process id
   * @throws IOException exception
   */
  public static void shutdownByCmd(String pid) throws IOException {
    if (!TestHelper.isUTPattern()) {
      Runtime.getRuntime().exec("kill -9 " + pid);
    }
  }

  public static String getJobID() {
    String jobID = System.getenv("RAY_JOB_ID");
    return jobID == null ? "default" : jobID;
  }

  /**
   * Execute an external command.
   *
   * @return Whether the command succeeded.
   */
  public static boolean executeCommand(List<String> command, int waitTimeoutSeconds) {
    try {
      ProcessBuilder processBuilder =
          new ProcessBuilder(command)
              .redirectOutput(ProcessBuilder.Redirect.INHERIT)
              .redirectError(ProcessBuilder.Redirect.INHERIT);
      Process process = processBuilder.start();
      boolean exit = process.waitFor(waitTimeoutSeconds, TimeUnit.SECONDS);
      if (!exit) {
        process.destroyForcibly();
      }
      return process.exitValue() == 0;
    } catch (Exception e) {
      throw new RuntimeException("Error executing command " + String.join(" ", command), e);
    }
  }
}
