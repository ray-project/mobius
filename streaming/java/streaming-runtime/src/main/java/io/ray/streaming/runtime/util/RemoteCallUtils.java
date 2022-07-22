package io.ray.streaming.runtime.util;

import com.google.protobuf.ByteString;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.WaitResult;
import io.ray.api.id.ActorId;
import io.ray.runtime.actor.NativeJavaActorHandle;
import io.ray.runtime.exception.UnreconstructableException;
import io.ray.streaming.runtime.generated.RemoteCall;
import io.ray.streaming.runtime.generated.RemoteCall.ActorHandle;
import io.ray.streaming.runtime.worker.context.JobWorkerContext;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;

public final class RemoteCallUtils {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteCallUtils.class);

  private RemoteCallUtils() {}

  /**
   * Ray creates actor asynchronously, so how could we get to know whether the actor is ready or
   * not? We did this by submitting an actor task and wait util it is ready, then call ray.get to
   * fetch the task execution result.
   *
   * @param rayObject ray object return by ray.call
   * @param actorId the specified actor id
   * @param <T> ray.call return type holding by ray object
   * @return true if actor is ready
   * @throws UnreconstructableException if actor was reconstructed, this exception might be raised,
   *     we need to do retry logic outside this method
   */
  public static <T> boolean waitUntilReturn(ObjectRef<T> rayObject, ActorId actorId)
      throws UnreconstructableException {
    return waitUntilReturn(rayObject, actorId, 10 * 1000);
  }

  public static <T> boolean waitUntilReturn(ObjectRef<T> rayObject, ActorId actorId, int timeout)
      throws UnreconstructableException {
    LOG.debug("Waiting rayObject in calling actor: {}.", actorId);
    List<ObjectRef<T>> rayObjectList = new ArrayList<>();
    rayObjectList.add(rayObject);

    WaitResult<T> result = Ray.wait(rayObjectList, rayObjectList.size(), timeout);
    if (!result.getUnready().isEmpty()) {
      LOG.error("Waiting actor unready size: {}, actor: {}.", result.getUnready().size(), actorId);
      return false;
    }
    return true;
  }

  /* An util method used to batch execute. */
  public static <T> boolean asyncBatchExecute(
      Function<T, Boolean> operation, List<T> executionVertices) {

    final Object asyncContext = Ray.getAsyncContext();
    final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    List<CompletableFuture<Boolean>> futureResults =
        executionVertices.stream()
            .map(
                vertex ->
                    CompletableFuture.supplyAsync(
                        () -> {
                          Ray.setAsyncContext(asyncContext);
                          // Note, we have no strong way to make 100% sure that these threads use
                          // the
                          // creater thread's class loader. Set the class loader manully here to
                          // avoid
                          // more class loader issues.
                          final ClassLoader oldClassLoader =
                              Thread.currentThread().getContextClassLoader();
                          Thread.currentThread().setContextClassLoader(classLoader);
                          Boolean result = operation.apply(vertex);
                          Thread.currentThread().setContextClassLoader(oldClassLoader);
                          return result;
                        }))
            .collect(Collectors.toList());

    List<Boolean> succeeded =
        futureResults.stream().map(CompletableFuture::join).collect(Collectors.toList());

    if (succeeded.stream().anyMatch(x -> !x)) {
      return false;
    }
    return true;
  }

  public static byte[] buildBytesWorkerContext(JobWorkerContext ctx) {
    LOG.info("Build python worker context bytes from context: {}.", ctx);

    RemoteCall.ActorId workerId =
        RemoteCall.ActorId.newBuilder()
            .setId(ByteString.copyFrom(ctx.getWorkerActorId().getBytes()))
            .build();

    Map<String, ActorHandle> inputQueueActor = new HashMap<>();
    ctx.getInputActors()
        .forEach(
            (queueName, actor) -> {
              final byte[] stateBytes = ((NativeJavaActorHandle) actor).toBytes();
              RemoteCall.ActorHandle actorHandle =
                  RemoteCall.ActorHandle.newBuilder()
                      .setFromJava(true)
                      .setActorHandle(ByteString.copyFrom(stateBytes))
                      .build();
              inputQueueActor.put(queueName, actorHandle);
            });

    Map<String, RemoteCall.ActorHandle> outputQueueActor = new HashMap<>();
    ctx.getOutputActors()
        .forEach(
            (queueName, actor) -> {
              final byte[] stateBytes = ((NativeJavaActorHandle) actor).toBytes();
              RemoteCall.ActorHandle actorHandle =
                  RemoteCall.ActorHandle.newBuilder()
                      .setFromJava(true)
                      .setActorHandle(ByteString.copyFrom(stateBytes))
                      .build();
              outputQueueActor.put(queueName, actorHandle);
            });

    ctx.getMasterActor().getId();
    final byte[] stateBytes = ((NativeJavaActorHandle) ctx.getMasterActor()).toBytes();
    RemoteCall.ActorHandle actorHandle =
        RemoteCall.ActorHandle.newBuilder()
            .setFromJava(true)
            .setActorHandle(ByteString.copyFrom(stateBytes))
            .build();

    // FIXME: duplicated serialization operating here.
    RemoteCall.JobWorkerContext ctxPb =
        RemoteCall.JobWorkerContext.newBuilder()
            .setId(ctx.getExecutionVertex().getExecutionVertexId())
            .setJobName(ctx.getJobName())
            .setOpName(ctx.getOpName())
            .setWorkerName(ctx.getWorkerName())
            .setWorkerId(workerId)
            .setMaster(actorHandle)
            .setIsChanged(ctx.isChanged())
            .setWorkerState(
                RemoteCall.ExecutionVertexState.forNumber(ctx.getExecutionVertex().getState().code))
            .putAllConf(ctx.getJobConf())
            .putAllInputQueueActor(inputQueueActor)
            .putAllOutputQueueActor(outputQueueActor)
            .build();

    return ctxPb.toByteArray();
  }
}
