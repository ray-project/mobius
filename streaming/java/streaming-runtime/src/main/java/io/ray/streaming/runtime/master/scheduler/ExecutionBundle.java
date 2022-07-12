package io.ray.streaming.runtime.master.scheduler;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import io.ray.api.placementgroup.Bundle;
import io.ray.api.placementgroup.PlacementGroup;
import io.ray.runtime.generated.Common.Bundle;
import io.ray.streaming.common.config.ResourceConfig;
import io.ray.streaming.common.enums.ResourceKey;
import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;
import io.ray.streaming.runtime.core.resource.ResourceState;
import io.ray.streaming.runtime.util.ResourceUtil;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.aeonbits.owner.ConfigFactory;

/**
 * Relation of {@link ExecutionVertex} and bundle.
 */
public class ExecutionBundle implements Serializable {

  public static final String MEM_KEY_FOR_GCS_SCHEDULING = "memory";
  public static final double MEM_DEFAULT = ConfigFactory.create(ResourceConfig.class).workerMemMbRequired();

  /**
   * Bundle id. Use execution vertex's id directly.
   */
  private final int id;

  /**
   * Resources required for bundle.
   */
  private Map<String, Double> resources;

  /**
   * Group who belongs to.
   */
  private int groupId = -1;

  /**
   * Index in group.
   */
  private int index = -1;

  /**
   * Ray bundle and placement group instance.
   */
  private Bundle bundle;
  private PlacementGroup placementGroup;

  /**
   * State of the current execution bundle.
   * IN_USE: bundle exist in placement group
   * TO_RELEASE: bundle is removed from placement group
   */
  private ResourceState resourceState = ResourceState.UNKNOWN;

  public ExecutionBundle(int id) {
    this(id, Collections.emptyMap());
  }

  public ExecutionBundle(int id, Map<String, Double> resources) {
    Preconditions.checkArgument(id >= 0, "illegal bundle id");
    this.id = id;
    this.resources = new HashMap<>(resources);
  }

  public int getId() {
    return id;
  }

  public String getFullId() {
    return groupId + "-" + id;
  }

  public void setResources(Map<String, Double> resources) {
    this.resources = resources;
  }

  public int getGroupId() {
    return groupId;
  }

  public int getIndex() {
    return index;
  }

  protected void setGroupInfo(int groupId, int index) {
    this.groupId = groupId;
    this.index = index;
  }

  protected void clearGroupInfo() {
    this.index = -1;
    this.groupId = -1;
  }

  public Map<String, Double> getResources() {
    return getResources(null);
  }

  public Map<String, Double> getResources(Map<String, String> jobConf) {
    // get default memory value first
    double memValue;
    if(jobConf == null || !jobConf.containsKey(ResourceConfig.WORKER_MEM)) {
      memValue = MEM_DEFAULT;
    } else {
      memValue = Double.parseDouble(jobConf.get(ResourceConfig.WORKER_MEM));
    }

    // override by spec
    if (resources.containsKey(ResourceKey.MEM.name())) {
      memValue = resources.get(ResourceKey.MEM.name());
    }

    // replace mem value because: 'Resource 'memory' must be specified in bundles if gcs scheduler enabled.'
    resources.put(MEM_KEY_FOR_GCS_SCHEDULING, (double) ResourceUtil.getMemoryMbFromMemoryValue(memValue));

    return new HashMap<>(resources);
  }

  protected void setPlacementGroup(PlacementGroup placementGroup) {
    this.placementGroup = placementGroup;
    this.resourceState = ResourceState.IN_USE;
  }

  public boolean isCreated() {
    return bundle != null;
  }

  public boolean isReadyToRelease() {
    return resourceState == ResourceState.TO_RELEASE;
  }

  public Bundle getBundle() {
    return bundle;
  }

  public PlacementGroup getPlacementGroup() {
    return placementGroup;
  }

  public ResourceState getResourceState() {
    return resourceState;
  }

  public void setToReleaseStatus() {
    resourceState = ResourceState.TO_RELEASE;
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (object == null || getClass() != object.getClass()) {
      return false;
    }
    ExecutionBundle that = (ExecutionBundle) object;
    return id == that.id;
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("id", id)
        .add("index", index)
        .add("resources", resources)
        .add("groupIndex", groupId + "-" + index)
        .toString();
  }

}
