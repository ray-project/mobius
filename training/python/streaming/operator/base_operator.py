import logging
from abc import ABCMeta, abstractmethod

logger = logging.getLogger(__name__)


class BaseOperator(metaclass=ABCMeta):
    """
    Dynamic Module, methods like spring bean
    """

    @abstractmethod
    def init(self, config):
        pass

    @abstractmethod
    def destroy(self):
        pass

    @abstractmethod
    def run(self):
        pass

    @abstractmethod
    def process(self, msg):
        """
        :param msg: data value
        :return:
        """
        pass

    @abstractmethod
    def save_checkpoint(self, checkpoint_id, barrier_id=None):
        """
        :param checkpoint_id: long, checkpoint id
        :param barrier_id: long, barrier id
        :return: byte[], operator state
        """
        pass

    @abstractmethod
    def save_checkpoint_async(self, checkpoint_id, barrier_id=None):
        """
        :param checkpoint_id: long, checkpoint id
        :param barrier_id: long, barrier id
        :return: byte[], operator state
        """
        pass

    @abstractmethod
    def load_checkpoint(self, checkpoint_id):
        """
        :param checkpoint_id: long, checkpoint id
        :return
        """
        pass
