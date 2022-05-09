package io.ray.streaming.runtime.master.joblifecycle;

import io.ray.api.id.ActorId;
import io.ray.streaming.runtime.command.EndOfDataReport;
import io.ray.streaming.runtime.master.JobMaster;
import io.ray.streaming.runtime.master.coordinator.BaseCoordinator;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobLifeCycleCoordinator extends BaseCoordinator {
    private static final Logger LOG = LoggerFactory.getLogger(JobLifeCycleCoordinator.class);
    private final Set<ActorId> unFinishedActors = new HashSet<>();
    private JobStatusListener jobStatusListener = null;

    public JobLifeCycleCoordinator(JobMaster jobMaster) {
        super(jobMaster);
        unFinishedActors.addAll(graphManager.getExecutionGraph().getAllActorsId());
    }

    @Override
    public void run() {
        while (!closed) {
            try {
                // This thread will be joined in BaseCoordinator for 30s.
                final EndOfDataReport command = runtimeContext.lifeCycleCmds.poll(2, TimeUnit.SECONDS);
                if (null == command) {
                    continue;
                }
                if (!unFinishedActors.contains(command.fromActorId)) {
                    LOG.warn("Invalid EndOfData report, skipped. actor id: {}", command.fromActorId);
                    continue;
                }
                LOG.info("Received EndOfData from {}", command.fromActorId);
                unFinishedActors.remove(command.fromActorId);
                if (unFinishedActors.isEmpty()) {
                    LOG.warn("Received EndOfData from all actors, job end.");
                    if (jobStatusListener != null) {
                        jobStatusListener
                            .resetStatus(JobStatus.FINISHED, System.currentTimeMillis());
                    }
                    break;
                }
            } catch (InterruptedException e) {
                LOG.error(e.getMessage());
            }
        }
        LOG.info("LifeCycle coordinator thread exit.");
    }

    public void reportEndOfData(EndOfDataReport report) {
        LOG.info("Report EndOfData commit {}.", report);
        runtimeContext.lifeCycleCmds.offer(report);
    }

    public void registerJobStatusListener(JobStatusListener listener) {
        jobStatusListener = listener;
    }

    public interface JobStatusListener {
        void resetStatus(JobStatus status, long timestamp);
    }
}
