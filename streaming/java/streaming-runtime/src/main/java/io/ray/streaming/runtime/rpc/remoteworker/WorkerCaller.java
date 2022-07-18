package io.ray.streaming.runtime.rpc.remoteworker;

import io.ray.api.BaseActorHandle;
import io.ray.api.ObjectRef;
import io.ray.streaming.runtime.core.checkpoint.Barrier;
import io.ray.streaming.runtime.core.checkpoint.PartialBarrier;
import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;
import io.ray.streaming.runtime.worker.context.JobWorkerContext;

public interface WorkerCaller {

  ExecutionVertex getExecutionVertex() throws NullPointerException;

  BaseActorHandle getActorHandle();

  String getConfig();

  BaseActorHandle create();

  ObjectRef<Boolean> init();

  void initControllerCaller();

  ObjectRef healthCheck();

  ObjectRef registerContext(JobWorkerContext ctx);

  ObjectRef<Boolean> applyNewContext();

  ObjectRef rollback(Long globalCheckpointId, Long partialCheckpointId);

  ObjectRef checkIfNeedRollback();

  ObjectRef commit(Barrier barrier);

  ObjectRef clearExpiredCp(Long stateCheckpointId, Long queueCheckpointId,
                           Long lastPartialCheckpointId);

  ObjectRef notifyCheckpointTimeout(Long checkpointId);

  ObjectRef destroy();

  void shutdownWithoutReconstruction();

  void shutdown();

  ObjectRef clearPartialCheckpoint(Long globalCheckpointId, Long partialCheckpointId);

  ObjectRef suspend();

  ObjectRef resume();

  ObjectRef fetchMetrics();

  ObjectRef rescaleRollback(Long partialCheckpointId);

  ObjectRef taskHealthCheck();

  ObjectRef broadcastPartialBarrier(PartialBarrier partialBarrier);

  ObjectRef isReadyRescaling();

  ObjectRef getContainerResourceUtilization();

  ObjectRef getSystemProperties();

  ObjectRef fetchProfilingInfo();

  ObjectRef<Boolean> forwardCommand(String command);
}
