package io.ray.streaming.runtime.core.graph.executiongraph;

import com.alipay.streaming.runtime.master.scheduler.hybrid.ActorRoleType;
import com.alipay.streaming.runtime.udc.controllerCaller.JavaWorkerUnitedDistributedControllerCallerImpl;
import com.alipay.streaming.runtime.udc.controllerCaller.PyWorkerUnitedDistributedControllerCallerImpl;
import com.alipay.streaming.runtime.udc.controllerCaller.UnitedDistributedControllerCaller;
import com.alipay.streaming.runtime.worker.WorkerRuntimeInfo;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import io.ray.api.BaseActorHandle;
import io.ray.api.ObjectRef;
import io.ray.streaming.api.Language;
import io.ray.streaming.common.generated.Common;
import io.ray.streaming.jobgraph.Language;
import io.ray.streaming.operator.BaseIndependentOperator;
import io.ray.streaming.runtime.master.scheduler.ActorRoleType;
import io.ray.streaming.runtime.master.scheduler.HealthCheckable;
import io.ray.streaming.runtime.util.ResourceUtil;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

/**
 * Independent vertex.
 * Working for something else independent of normal job worker
 */
public class IndependentVertex
    implements Serializable, HealthCheckable, UnitedDistributedControllerCaller {

  /**
   * actor role name . Broker or something else
   */
  protected final ActorRoleType roleName;

  /**
   * index of the same role actor
   */
  protected final int index;

  /**
   * language of the independent operator
   */
  protected Language language;

  /**
   * actor handler
   */
  protected BaseActorHandle actor;

  /**
   * caller of united distributed controller
   */
  protected UnitedDistributedControllerCaller unitedDistributedControllerCaller;

  /**
   * resource of this actor
   */
  protected Map<String, Double> requiredResources;

  /**
   * Config(map) for Independent vertex.
   */
  protected Map<String, String> actorConf;

  private String moduleName;

  private String className;

  private String name;

  /**
   * Runtime info for independent actor.
   */
  protected WorkerRuntimeInfo runtimeInfo;

  protected ExecutionVertexState independentVertexState;

  public IndependentVertex(Builder builder){
    this.roleName = builder.roleName;
    this.index = builder.index;
    this.requiredResources = ResourceUtil.formatResource(builder.requiredResources);
    this.actorConf = builder.actorConf;
    this.moduleName = builder.moduleName;
    this.className = builder.className;
    this.language = builder.language;
    this.name = builder.name;
    this.independentVertexState = ExecutionVertexState.TO_ADD;
  }

  public static BaseIndependentOperator buildIndependentOperator(String className, Map<String, String> config) throws Exception {
      Class actorClass = Class.forName(className);
      Constructor<?>[] constructors = actorClass.getConstructors();
      Preconditions.checkArgument(constructors.length == 1,
        "Independent operator only support at most 1 constructor instead of %s", constructors.length);
      Constructor ctor = constructors[0];
      Object ret = ctor.newInstance(config);
      Preconditions.checkState(ret instanceof BaseIndependentOperator, "Independent operator class must be sub class of BaseIndependentOperator ");
      return (BaseIndependentOperator)ret;
  }

  /**
   * attach specify actor to this
   */
  public void attachActor(BaseActorHandle actor) {
    this.actor = actor;
    this.independentVertexState = ExecutionVertexState.RUNNING;
    initControllerCaller();
  }

  public String getModuleName() {
    return moduleName;
  }

  public String getClassName() {
    return className;
  }

  /**
   * Getter method for property <tt>index</tt>.
   *
   * @return property value of index
   */
  public int getIndex() {
    return index;
  }

  /**
   * Getter method for property <tt>roleName</tt>.
   *
   * @return property value of roleName
   */
  public ActorRoleType getRoleName() {
    return roleName;
  }

  /**
   * Getter method for property <tt>actor</tt>.
   *
   * @return property value of actor
   */
  @Override
  public BaseActorHandle getActorHandle() {
    return actor;
  }

  public String getActorName() {
    return roleName.name();
  }

  /**
   * Getter method for property <tt>requiredResources</tt>.
   *
   * @return property value of requiredResources
   */
  public Map<String, Double> getRequiredResources() {
    return requiredResources;
  }

  public void updateResource(String resourceKey, Double resouceValue) {
    if (!StringUtils.isEmpty(resourceKey) && resouceValue > 0) {
      requiredResources.put(resourceKey, resouceValue);
    }
  }

  /**
   * Getter method for property <tt>actorConf</tt>.
   *
   * @return property value of actorConf
   */
  public Map<String, String> getActorConf() {
    return actorConf;
  }

  public String getName() {
    if (StringUtils.isEmpty(name)) {
      return getRoleName().getDesc() + "-" + getIndex();
    }
    return name + "-" + getIndex();
  }

  public ExecutionVertexState getIndependentVertexState() {
    return this.independentVertexState;
  }

  public void setIndependentVertexState(ExecutionVertexState state) {
    this.independentVertexState = state;
  }

  public WorkerRuntimeInfo getRuntimeInfo() {
    return runtimeInfo;
  }

  public void updateRuntimeInfo(WorkerRuntimeInfo runtimeInfo) {
    if (this.runtimeInfo == null) {
      this.runtimeInfo = new WorkerRuntimeInfo();
    }
    this.runtimeInfo.update(runtimeInfo);
  }

  public String getRuntimePid() {
    if (runtimeInfo != null) {
      return runtimeInfo.getPid();
    }
    return "";
  }

  public String getRuntimeHostname() {
    if (runtimeInfo != null) {
      return runtimeInfo.getHostname();
    }
    return "";
  }


  public String getRuntimeIpAddress() {
    if (runtimeInfo != null) {
      return runtimeInfo.getIpAddress();
    }
    return "";
  }

  public Language getLanguage() {
    return language;
  }

  @Override
  public boolean healthCheckable() {
    return false;
  }

  /**
   * IndependentActor builder
   */
  public static class Builder {
    private final ActorRoleType roleName;
    private final Language language;
    private final int index;

    private Map<String, Double> requiredResources = new HashMap<>();
    private Map<String, String> actorConf = new HashMap<>();

    private String moduleName;
    private String className;
    private String name;


    public Builder(ActorRoleType roleName, int index, Language language) {
      this.roleName = roleName;
      this.index = index;
      this.language = language;
    }

    public Builder moduleName(String moduleName) {
      this.moduleName = moduleName;
      return this;
    }

    public Builder className(String className) {
      this.className = className;
      return this;
    }

    public Builder name(String name) {
      this.name = name;
      return this;
    }

    public Builder requiredResources(Map<String, Double> requiredResources) {
      this.requiredResources = requiredResources;
      return this;
    }

    public Builder actorConf(Map<String, String> actorConf) {
      this.actorConf = actorConf;
      return this;
    }


    public IndependentVertex build(){
      return new IndependentVertex(this);
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("actor", actor.getId())
        .add("roleName", roleName)
        .add("moduleName", moduleName)
        .add("className", className)
        .add("index", index)
        .toString();
  }

  public void destroy() {
    if (actor != null) {
      actor.kill(true);
    }
  }

  /**
   * Initialize united distributed controller caller.
   */
  public void initControllerCaller() {
    switch (language) {
      case JAVA:
        unitedDistributedControllerCaller =
            new JavaWorkerUnitedDistributedControllerCallerImpl(getActorHandle());
        break;
      case PYTHON:
        unitedDistributedControllerCaller =
            new PyWorkerUnitedDistributedControllerCallerImpl(getActorHandle());
        break;
      default:
        throw new IllegalArgumentException(String
            .format("UnitedDistributedControllerCaller language error, not support language type=%s",
                language));
    }
  }


  @Override
  public ObjectRef controllerPrepare(Common.UnitedDistributedControlMessage controlMessage) {
    return unitedDistributedControllerCaller.controllerPrepare(controlMessage);
  }

  @Override
  public void controllerCommit() {
    unitedDistributedControllerCaller.controllerCommit();
  }

  @Override
  public void controllerDispose() {
    unitedDistributedControllerCaller.controllerDispose();
  }

  @Override
  public void controllerCancel() {
    unitedDistributedControllerCaller.controllerCancel();
  }
}
