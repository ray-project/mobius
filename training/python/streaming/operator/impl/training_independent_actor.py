from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import sys

from ray.streaming.generated import remote_call_pb2
from ray.streaming.runtime.worker import WorkerRuntimeInfo
from raystreaming.runtime.udc_adapter_callee import (
    UnitedDistributedControllerAdapterCallee as UDCAdapterCallee,
)
import ray

logger = logging.getLogger(__name__)


class TrainingIndependentActorInterface(Object):
    def __init__(self, config):
        self._is_health = True
        if ray.get_runtime_context().was_current_actor_reconstructed:
            logger.info(f"Actor: {ray.get_runtime_context().actor_id} is restarting.")
            self._is_health = False
        super().__init__(config)

    def health_check(self):
        worker_runtime_info = WorkerRuntimeInfo()
        logger.debug("Get worker runtime info: {}.".format(worker_runtime_info))

        try:
            info_result_pb = remote_call_pb2.WorkerRuntimeInfo()

            info_result_pb.healthy = self._is_health
            if not self._is_health:
                self._is_health = True
            info_result_pb.cluster_name = worker_runtime_info.get_cluster_name
            info_result_pb.idcName = worker_runtime_info.get_idc_name
            info_result_pb.pid = worker_runtime_info.get_pid
            info_result_pb.hostname = worker_runtime_info.get_hostname
            info_result_pb.ip_address = worker_runtime_info.get_ip_address

            return info_result_pb.SerializeToString()
        except Exception as e:
            logger.warning("Get worker runtime info occurs an exception: {}".format(e))
            return remote_call_pb2.WorkerRuntimeInfo().SerializeToString()

    def inject_exception(self, exception_injector_bytes):
        sys.exit(1)
