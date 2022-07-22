package io.ray.streaming.runtime.master.scheduler;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import io.ray.api.PlacementGroups;
import io.ray.api.options.PlacementGroupCreationOptions;
import io.ray.api.placementgroup.PlacementGroup;
import io.ray.api.placementgroup.PlacementStrategy;
import io.ray.streaming.runtime.core.resource.ResourceState;
import io.ray.streaming.runtime.util.LoggerFactory;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

/** Placement group info, including name, bundles and strategy. */
public class ExecutionGroup implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(ExecutionGroup.class);

  /** Group id. */
  private final int groupId;

  /** Use id and job name as group name. e.g. 1-job_name */
  private final String groupName;

  /** How many bundles is expected. */
  private int expectSize = -1;

  /** Placement strategy. Please refers to {@link PlacementStrategy}. */
  private PlacementStrategy placementStrategy;

  /** Bundles info. */
  private List<ExecutionBundle> executionBundles;

  /** Placement group for ray using. */
  private PlacementGroup placementGroup;

  /**
   * State of the current execution group. IN_USE: placement group is in used TO_RELEASE: placement
   * group is removed
   */
  private ResourceState resourceState = ResourceState.UNKNOWN;

  private AtomicInteger bundleIndex = new AtomicInteger(0);

  public ExecutionGroup(int groupId, String jobName) {
    this(groupId, jobName, PlacementStrategy.PACK);
  }

  public ExecutionGroup(int groupId, String jobName, PlacementStrategy placementStrategy) {
    this(groupId, jobName, placementStrategy, new ArrayList<>());
  }

  public ExecutionGroup(
      int groupId,
      String jobName,
      PlacementStrategy placementStrategy,
      List<ExecutionBundle> executionBundles) {
    Preconditions.checkArgument(groupId >= 0, "illegal group id");
    Preconditions.checkArgument(!StringUtils.isEmpty(jobName), "illegal group name");
    Preconditions.checkNotNull(placementStrategy, "illegal placement strategy");
    Preconditions.checkArgument(executionBundles != null, "illegal bundles");
    this.groupId = groupId;
    this.groupName = jobName + "-" + groupId;
    this.placementStrategy = placementStrategy;
    this.executionBundles = executionBundles;
  }

  public int getGroupId() {
    return groupId;
  }

  public String getGroupName() {
    return groupName;
  }

  public int getExpectSize() {
    return expectSize;
  }

  public void setExpectSize(int expectSize) {
    this.expectSize = expectSize;
  }

  public boolean reachExpectSize() {
    Preconditions.checkArgument(
        expectSize > 0, "Expect size should > 0 if use function 'reachExpectSize'.");
    if (getSize() < expectSize) {
      return false;
    }
    return true;
  }

  public PlacementStrategy getPlacementStrategy() {
    return placementStrategy;
  }

  public void setPlacementStrategy(PlacementStrategy placementStrategy) {
    this.placementStrategy = placementStrategy;
  }

  public List<ExecutionBundle> getBundles() {
    return executionBundles;
  }

  public int getSize() {
    if (executionBundles != null) {
      return executionBundles.size();
    }
    return 0;
  }

  public synchronized void addBundle(ExecutionBundle executionBundle) {
    executionBundle.setGroupInfo(groupId, getNextBundleIndex());
    this.executionBundles.add(executionBundle);
  }

  public synchronized void removeBundle(ExecutionBundle executionBundle) {
    executionBundle.clearGroupInfo();
    this.executionBundles.remove(executionBundle);
  }

  public synchronized void removeBundle(int executionVertexId) {
    this.executionBundles.stream()
        .filter(executionBundle -> executionBundle.getId() == executionVertexId)
        .forEach(ExecutionBundle::clearGroupInfo);
    this.executionBundles.removeIf(executionBundle -> executionBundle.getId() == executionVertexId);
  }

  public PlacementGroup getPlacementGroup() {
    return placementGroup;
  }

  public int getBundleIndex() {
    return bundleIndex.get();
  }

  private int getNextBundleIndex() {
    return bundleIndex.getAndIncrement();
  }

  public ResourceState getResourceState() {
    return resourceState;
  }

  /**
   * Build placement group according to the execution group info.
   *
   * @return placement group
   */
  public PlacementGroup buildPlacementGroup() {
    //    int timeoutSecond = Integer.parseInt(
    //        jobConf.getOrDefault(SchedulerConfig.RESCALING_PLACEMENTGROUP_WAIT_TIMEOUT_S,
    //            SchedulerConfig.RESCALING_PLACEMENTGROUP_WAIT_TIMEOUT_S_DEFAULT));

    if (placementGroup == null) {
      PlacementGroupCreationOptions options =
          new PlacementGroupCreationOptions.Builder()
              .setName(groupName)
              .setBundles(
                  getBundles().stream()
                      .map(ExecutionBundle::getResources)
                      .collect(Collectors.toList()))
              .setStrategy(getPlacementStrategy())
              .build();

      LOG.info("Create placement group: {} with bundles: {}.", groupName, executionBundles);
      placementGroup = PlacementGroups.createPlacementGroup(options);
      executionBundles.forEach(
          executionBundle -> executionBundle.setPlacementGroup(placementGroup));
      resourceState = ResourceState.IN_USE;
    } else {
      List<ExecutionBundle> notCreatedBundles =
          executionBundles.stream()
              .filter(executionBundle -> !executionBundle.isCreated())
              .collect(Collectors.toList());
      LOG.error(
          "Not support add bundle into an exist PlacementGroup! new bundles:{}", notCreatedBundles);
    }
    return placementGroup;
  }

  /** Update inner bundles and update self statement. */
  public void refresh() {
    if (executionBundles == null || placementGroup == null) {
      LOG.error("Can not refresh bundles when placement group or bundle is null.");
      return;
    }
    refreshBundles();

    if (executionBundles.isEmpty()) {
      LOG.info("Remove placement group: {}.", placementGroup.getId());
      PlacementGroups.removePlacementGroup(placementGroup.getId());
      resourceState = ResourceState.TO_RELEASE;
    }
  }

  private void refreshBundles() {
    // remove unused bundles from pg

    List<ExecutionBundle> toBeReleaseBundles =
        executionBundles.stream()
            .filter(ExecutionBundle::isReadyToRelease)
            .collect(Collectors.toList());
    if (!toBeReleaseBundles.isEmpty()) {
      LOG.error(
          "Not support remove bundle from placement group. The bundles are: {}",
          toBeReleaseBundles);
    }
    LOG.info("Remove bundles: {} from the bundle list.", toBeReleaseBundles);
    executionBundles.removeIf(ExecutionBundle::isReadyToRelease);
  }

  public void removePlacementGroup() {
    //    if (executionBundles != null) {
    //      executionBundles.forEach(executionBundle ->
    // placementGroup.removeBundles(ImmutableList.of(executionBundle.getIndex())));
    //    }
    PlacementGroups.removePlacementGroup(placementGroup.getId());
    LOG.info(
        "Placement group: {} and all it's bundles has been removed.", placementGroup.getName());
  }

  public void updateBundleIndex(int index) {
    bundleIndex = new AtomicInteger(index);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("groupId", groupId)
        .add("groupName", groupName)
        .add("placementStrategy", placementStrategy)
        .add(
            "bundles",
            executionBundles.stream().map(ExecutionBundle::getId).collect(Collectors.toList()))
        .toString();
  }
}
