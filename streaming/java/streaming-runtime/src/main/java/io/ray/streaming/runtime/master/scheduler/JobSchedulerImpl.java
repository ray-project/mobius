package io.ray.streaming.runtime.master.scheduler;

import io.ray.api.ActorHandle;
import io.ray.streaming.common.config.JvmConfig;
import io.ray.streaming.common.config.ResourceConfig;
import io.ray.streaming.common.enums.ResourceKey;
import io.ray.streaming.common.serializer.KryoUtils;
import io.ray.streaming.runtime.config.StreamingConfig;
import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionGraph;
import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionJobVertex;
import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;
import io.ray.streaming.runtime.core.resource.Container;
import io.ray.streaming.runtime.master.JobMaster;
import io.ray.streaming.runtime.master.event.EventMessage;
import io.ray.streaming.runtime.master.graphmanager.GraphManager;
import io.ray.streaming.runtime.master.joblifecycle.JobStatus;
import io.ray.streaming.runtime.master.resourcemanager.ResourceManager;
import io.ray.streaming.runtime.master.resourcemanager.ViewBuilder;
import io.ray.streaming.runtime.master.scheduler.controller.WorkerLifecycleController;
import io.ray.streaming.runtime.master.scheduler.strategy.PlacementGroupAssignStrategy;
import io.ray.streaming.runtime.master.scheduler.strategy.PlacementGroupAssignStrategyFactory;
import io.ray.streaming.runtime.master.scheduler.strategy.PlacementGroupAssignStrategyType;
import io.ray.streaming.runtime.util.ResourceUtil;
import io.ray.streaming.runtime.worker.context.JobWorkerContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Job scheduler implementation. */
public class JobSchedulerImpl implements JobScheduler {

  private static final Logger LOG = LoggerFactory.getLogger(JobSchedulerImpl.class);
  protected final JobMaster jobMaster;
  protected final ResourceManager resourceManager;
  protected final GraphManager graphManager;
  protected final PlacementGroupAssignStrategy placementGroupAssignStrategy;
  protected final WorkerLifecycleController workerLifecycleController;
  protected StreamingConfig jobConfig;

  public JobSchedulerImpl(JobMaster jobMaster) {
    this.jobMaster = jobMaster;
    this.graphManager = jobMaster.getGraphManager();
    this.resourceManager = jobMaster.getResourceManager();
    this.placementGroupAssignStrategy = initPlacementGroupAssignStrategy();
    this.workerLifecycleController = new WorkerLifecycleController();
    this.jobConfig = jobMaster.getRuntimeContext().getConfig();

    LOG.info("Scheduler initiated.");
  }

  private PlacementGroupAssignStrategy initPlacementGroupAssignStrategy() {
    PlacementGroupAssignStrategyType placementGroupAssignStrategyType =
            PlacementGroupAssignStrategyType.valueOf(
                    jobConfig.getMasterConfig().schedulerConfig.placementGroupAssignStrategy().toUpperCase());

    PlacementGroupAssignStrategy placementGroupAssignStrategy = PlacementGroupAssignStrategyFactory
            .getStrategy(placementGroupAssignStrategyType);

    LOG.info("Placement group assign strategy initialized: {}.", placementGroupAssignStrategy.getName());
    return placementGroupAssignStrategy;
  }

  /**
   * The entrance of submitting a Job.
   *
   * @return Whether the schedule works or not.
   */
  public boolean scheduleJob() throws Exception {
    LOG.info("Start scheduling job.");

    // 1. prepare, including update required resources and create workers
    ScheduleResult result = prepareJobSubmission();
    if (!result.result()){
      // 3. destroy job
      LOG.error("Failed to prepare job submission: {}.", result.getExceptionMsg());
      boolean destroyRes = destroyJob(JobStatus.SUBMITTING_FAILED);
      if (!destroyRes){
        LOG.error("Failed to destroy job, throwing exception.");
        // 4. Interrupt the job by throwing an exception
        markInterrupted("Failed to destroy job.");
      }
      return false;
    }

    // 2.
    result = doJobSubmission();
    if (!result.result()){
      // 3. destroy job
      LOG.error("Failed to doJobSubmission: {}.", result.getExceptionMsg());
      boolean destroyRes = destroyJob(JobStatus.SUBMITTING_FAILED);
      if (!destroyRes){
        LOG.error("Failed to destroy job, throwing exception.");
        // 4. Interrupt the job by throwing an exception
        markInterrupted("Failed to destroy job.");
      }
      return false;
    }

    markFinished();
    LOG.info("Finish scheduling job.");
    return true;
  }

  @Override
  public ScheduleResult prepareJobSubmission() {
    ExecutionGraph executionGraph = graphManager.getExecutionGraph();
    LOG.info("Start to schedule job: {}.", executionGraph.getJobInformation().getJobName());

    // check and update resources
    if (!checkAndUpdateResources(executionGraph)) {
      return ScheduleResult.fail(EventMessage.RESOURCE_CHECK_AND_UPDATE_FAIL.getDesc());
    }

    // assign workers
    try {
      placementGroupAssignStrategy.assignForScheduling(executionGraph);
    } catch (Exception e) {
      LOG.error("Error when assigning workers.", e);
      return ScheduleResult.fail(EventMessage.ASSIGN_WORKER_FAIL.getDesc());
    }

    // create workers
    boolean result = createWorkers(executionGraph);
    if (!result) {
      return ScheduleResult.fail(EventMessage.START_WORKER_FAIL.getDesc());
    }

    return ScheduleResult.success();
  }

  @Override
  public ScheduleResult doJobSubmission() {
    ExecutionGraph executionGraph;
    String jobName = null;

    try {
      executionGraph = graphManager.getExecutionGraph();
      jobName = executionGraph.getJobInformation().getJobName();

      run(executionGraph);
    } catch (Exception e) {
      LOG.error("Failed to schedule job {} by {}.", jobName, e.getMessage(), e);
      return ScheduleResult.fail(new ScheduleException(e));
    }

    jobMaster.resetStatus(JobStatus.RUNNING);

//    graphManager.updateOriginalAndClearChanged();
    jobMaster.saveContext();

    LOG.info("Scheduling job {} succeeded.", jobName);

    return ScheduleResult.success();
  }

  @Override
  public boolean destroyJob(JobStatus jobStatus) {
    LOG.info("Destroy all workers.");
    preActionBeforeDestroying();

    // remove all actors
    boolean workerDestroyResult;
    if (jobStatus == JobStatus.FINISHED_AND_CLEAN) {
      workerDestroyResult =
              workerLifecycleController.destroyWorkers(
                      graphManager.getExecutionGraph().getAllExecutionVertices());
    } else {
      workerDestroyResult =
              workerLifecycleController.destroyWorkersDirectly(
                      graphManager.getExecutionGraph().getAllExecutionVertices());
    }

    // remove all placement group
    graphManager.removeAllPlacementGroup();

    jobMaster.resetStatus(jobStatus);

    // deal with resubmit
    if (jobStatus == JobStatus.RESUBMITTING) {
      LOG.info("Save context before resubmission.");
      jobMaster.saveContext();
    }

    // unregister group listener
//    jobMaster.unRegisterGroupListener();

    return workerDestroyResult;
  }

  /**
   * Run the job after all actors has been allocated
   *
   * @param executionGraph the scheduled execution graph
   */
  protected void run(ExecutionGraph executionGraph) throws RuntimeException {
    try {
      Map<ExecutionVertex, JobWorkerContext> vertexToContextMap = buildJobWorkersContext(executionGraph);

      // Register worker context
      // init workers
      if (!initWorkers(vertexToContextMap)) {
        throw new RuntimeException(EventMessage.INIT_WORKER_FAIL.getDesc());
      }

      // Register master context
      if (!initMaster()) {
        throw new RuntimeException(EventMessage.INIT_MASTER_FAIL.getDesc());
      }

      // Start all workers to process data
      startWorkers(executionGraph, jobMaster.getRuntimeContext().lastCheckpointId);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Build workers context.
   *
   * @param executionGraph execution graph
   * @return vertex to worker context map
   */
  protected Map<ExecutionVertex, JobWorkerContext> buildJobWorkersContext(
      ExecutionGraph executionGraph) {
    ActorHandle<JobMaster> masterActor = jobMaster.getRuntimeContext().getJobMasterActor();

    // build workers' context
    Map<ExecutionVertex, JobWorkerContext> vertexToContextMap = new HashMap<>();
    executionGraph
        .getAllExecutionVertices()
        .forEach(
            vertex -> {
              JobWorkerContext context = buildJobWorkerContext(vertex, masterActor);
              vertexToContextMap.put(vertex, context);
            });
    return vertexToContextMap;
  }

//  /**
//   * Allocate job workers' resource then create job workers' actor.
//   *
//   * @param executionGraph the physical plan
//   */
//  protected void prepareResourceAndCreateWorker(ExecutionGraph executionGraph) {
//    List<Container> containers = resourceManager.getRegisteredContainers();
//
//    // Assign resource for execution vertices
//    resourceManager.assignResource(containers, executionGraph);
//
//    LOG.info("Allocating map is: {}.", ViewBuilder.buildResourceAssignmentView(containers));
//
//    // Start all new added workers
//    createWorkers(executionGraph);
//  }

  /**
   * Create JobWorker actors according to the physical plan.
   *
   * @param executionGraph physical plan
   * @return actor creation result
   */
  protected boolean createWorkers(ExecutionGraph executionGraph) {
    LOG.info("Begin creating workers.");
    long startTs = System.currentTimeMillis();

    // create JobWorker actors
    boolean createResult =
        workerLifecycleController.createWorkers(executionGraph);

    if (createResult) {
      LOG.info("Finished creating workers. Cost {} ms.", System.currentTimeMillis() - startTs);
      return true;
    } else {
      LOG.error("Failed to create workers. Cost {} ms.", System.currentTimeMillis() - startTs);
      return false;
    }
  }

  /**
   * Init JobWorkers according to the vertex and context infos.
   *
   * @param vertexToContextMap vertex - context map
   */
  protected boolean initWorkers(Map<ExecutionVertex, JobWorkerContext> vertexToContextMap) {
    boolean succeed;
    int timeoutMs = jobConfig.getMasterConfig().schedulerConfig.jobSubmissionWorkerWaitTimeoutMs();
    succeed = workerLifecycleController.initWorkers(vertexToContextMap, timeoutMs);
    if (!succeed) {
      LOG.error("Failed to initiate workers in {} milliseconds", timeoutMs);
    }
    return succeed;
  }

  /** Start JobWorkers according to the physical plan. */
  public boolean startWorkers(ExecutionGraph executionGraph, long checkpointId) {
    boolean result;
    try {
      result =
          workerLifecycleController.startWorkers(
              executionGraph,
              checkpointId,
              jobConfig.getMasterConfig().schedulerConfig.jobSubmissionWorkerWaitTimeoutMs());
    } catch (Exception e) {
      LOG.error("Failed to start workers.", e);
      return false;
    }
    return result;
  }

  private boolean initMaster() {
    return jobMaster.init(false);
  }

  private JobWorkerContext buildJobWorkerContext(
      ExecutionVertex executionVertex, ActorHandle<JobMaster> masterActor) {

    Map<Integer, ExecutionVertex> vertexIdExecutionVertexMap =
        graphManager.getExecutionGraph().getExecutionVertexIdExecutionVertexMap();
    byte[] vertexIdExecutionVertexMapBytes = KryoUtils.writeToByteArray(vertexIdExecutionVertexMap);

    // create java worker context
    JobWorkerContext context = new JobWorkerContext(masterActor, executionVertex, vertexIdExecutionVertexMapBytes);

    return context;
  }

  private void preActionBeforeDestroying() {
    // TODO
  }

  /**
   * Destroy job(all instances).
   *
   * @return destroying action result
   */
  public boolean destroyJob() {
    LOG.info("Destroy all workers.");
    preActionBeforeDestroying();

    // remove all actors
    boolean workerDestroyResult =
        workerLifecycleController.destroyWorkers(
            graphManager.getExecutionGraph().getAllExecutionVertices());

    return workerDestroyResult;
  }

  /**
   * Destroy JobWorkers according to the vertex infos.
   *
   * @param executionVertices specified vertices
   */
  public boolean destroyWorkers(List<ExecutionVertex> executionVertices) {
    boolean result;
    try {
      result = workerLifecycleController.destroyWorkers(executionVertices);
    } catch (Exception e) {
      LOG.error("Failed to destroy workers.", e);
      return false;
    }
    return result;
  }

  private void markInterrupted(String reason) throws Exception {
    throw new Exception(
        "JobScheduler interrupted, reason: " + reason);
  }

  private void markFinished() {
    // TODO
  }

  protected boolean checkResource(List<ExecutionJobVertex> executionJobVertices) {
    return executionJobVertices.stream().allMatch(executionJobVertex -> {
      Map<String, Double> resources = executionJobVertex.getJobVertex().getResources();
      String operatorName = executionJobVertex.getExecutionJobVertexName();

      for (Map.Entry<String, Double> resource : resources.entrySet()) {
        String resourceKey = resource.getKey();
        Double resourceValue = resource.getValue();

        if (StringUtils.isEmpty(resourceKey)) {
          LOG.error("Resource key is empty for operator: {}.", operatorName);
          return false;
        } else if (resourceValue < 0) {
          LOG.error("Resource value < 0 for operator: {}.", operatorName);
          return false;
        } else if (resourceKey.equals(ResourceKey.MEM.name())) {
          if (!ResourceUtil.isMemoryMbValue(resourceValue.longValue())) {
            LOG.error("Memory resource is illegal for operator: {}.", operatorName);
            return false;
          }
        }
      }
      return true;
    });
  }

  public boolean checkAndUpdateResources(ExecutionGraph executionGraph) {
    // check resource
    if (!checkResource(executionGraph.getAllExecutionJobVertices())) {
      LOG.error("Failed to check resource.");
      return false;
    }

    List<Container> containers = resourceManager.getRegisteredContainers();
    // Assign resource for execution vertices
    resourceManager.assignResource(containers, executionGraph);
    LOG.info("Allocating map is: {}.", ViewBuilder.buildResourceAssignmentView(containers));

    // update basic resource(cpu+mem)
    updateResource(executionGraph.getAllExecutionJobVertices());

    updateResourceForSpecificVertices();

    return true;
  }

  /**
   * Update resource for specific execution vertices.
   * In job scheduler class, nothing can be done in this function but
   * there will be changes in its subclass of scheduler.
   */
  protected void updateResourceForSpecificVertices() {
    // NOTE(lingxuan.zlx): it updates resource for dynamic py sink and must be after
    // update all execution job vertices.
  }

  /**
   * Set resources(global resource + jvm) for vertex
   */
  protected void updateResource(List<ExecutionJobVertex> executionJobVertices) {
    ResourceConfig resourceConfig = jobConfig.getMasterConfig().resourceConfig;

    // get global default resource required
    boolean isStrictLimit = resourceConfig.isWorkerResourceStrictLimit();
    double defaultCpuRequired = ResourceUtil.formatCpuValue(resourceConfig.workerCpuRequired());
    double defaultMemRequired = resourceConfig.workerMemMbRequired();
    double defaultGpuRequired = resourceConfig.workerGpuRequired();

    // get default proposed resource
    Map<String, Map<String, Double>> proposedOperatorResources =
            ResourceUtil.resolveOperatorResourcesFromJobConfig(resourceConfig.operatorProposedResource());

    // get global resource definition from job config
    Map<String, Map<String, Double>> operatorResourcesFromJobConfig =
            ResourceUtil.resolveOperatorResourcesFromJobConfig(resourceConfig.operatorCustomResource());

    executionJobVertices.forEach(executionJobVertex -> {
      Map<String, Double> resources = new HashMap<>(4);
      String operatorId = String.valueOf(executionJobVertex.getExecutionJobVertexId());
      String operatorName = executionJobVertex.getExecutionJobVertexName();

      // --------------------------------
      // set default required resource
      // --------------------------------
      if (isStrictLimit) {
        resources.put(ResourceKey.CPU.name(), defaultCpuRequired);
        resources.put(ResourceKey.GPU.name(), defaultGpuRequired);
        resources.put(ResourceKey.MEM.name(), defaultMemRequired);
      }

      // ----------------------------------------------------
      // override resource by default proposed op resource
      // ----------------------------------------------------
      if (!proposedOperatorResources.isEmpty() && resourceConfig.enableProposedResource()) {
        for (Map.Entry<String, Map<String, Double>> proposed : proposedOperatorResources
                .entrySet()) {
          String opNameKey = proposed.getKey();
          if (operatorName.contains(opNameKey)) {
            Map<String, Double> resource = proposedOperatorResources.get(opNameKey);
            ResourceUtil.formatResource(resource);
            LOG.info("Override resource by default proposed for operator: {}, resource: {}.",
                    operatorName, resource);
            resources.putAll(resource);
            break;
          }
        }
      }

      // ----------------------------------------------------
      // override resource by op custom resource from job config
      // ----------------------------------------------------
      if (!operatorResourcesFromJobConfig.isEmpty()) {
        Map<String, Double> resource = null;
        if (operatorResourcesFromJobConfig.containsKey(operatorName)) {
          resource = operatorResourcesFromJobConfig.get(operatorName);
        } else if (operatorResourcesFromJobConfig.containsKey(operatorId)) {
          resource = operatorResourcesFromJobConfig.get(operatorId);
        }

        if (resource != null) {
          ResourceUtil.formatResource(resource);
          LOG.info("Override resource from job config for operator: {}, resource: {}.",
                  operatorName, resource);
          resources.putAll(resource);
        }
      }

      // ----------------------------------------------------
      // override resource by op custom resource from op config
      // ----------------------------------------------------
      Map<String, Double> operatorResourcesFromOpConfig =
              executionJobVertex.getJobVertex().getResources();
      if (!operatorResourcesFromOpConfig.isEmpty()) {
        ResourceUtil.formatResource(operatorResourcesFromOpConfig);
        LOG.info("Override resource from op config for operator: {}, resource: {}.",
                operatorName, operatorResourcesFromOpConfig);
        resources.putAll(operatorResourcesFromOpConfig);
      }

      // --------------------------------
      // override mem if jvm opts is set
      // --------------------------------
      Double memoryMbFromJvmOpts = ResourceUtil.calculateMemoryMbFromJvmOptsStr(
              executionJobVertex.getOpConfig().getOrDefault(JvmConfig.JVM_OPTS, ""),
              operatorName);

      // override mem resource with jvm result
      Double currentMemoryMb = resources.get(ResourceKey.MEM.name());
      if (memoryMbFromJvmOpts > 0 && memoryMbFromJvmOpts > currentMemoryMb) {
        LOG.info("Override memory mb with: {} for operator: {} by jvm options.",
                memoryMbFromJvmOpts, operatorName);
        resources.put(ResourceKey.MEM.name(), memoryMbFromJvmOpts);
      }

      // update resource for all it's vertices
      LOG.info("Operator resources are: {}.", resources);
      executionJobVertex.updateResources(resources);
    });
  }

}
