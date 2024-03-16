from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging

import ray
import json

from streaming.operator.impl.training_independent_actor import TrainingIndependentActorInterface

logger = logging.getLogger(__name__)


@ray.remote(max_restarts=-1)
class MockOptimizerExecutor:
    def __init__(self, conf):
        logger.info(f"Mock auto scale execution init, config {conf}")
        conf_dict = json.loads(conf)
        logger.info(f"Decoded config : {conf_dict}")


class OptimizerInterface(TrainingIndependentActorInterface):
    def __init__(self, config=None):
        super().__init__(config)