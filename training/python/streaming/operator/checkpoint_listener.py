from abc import ABCMeta, abstractmethod


class CheckpointListener(metaclass=ABCMeta):
    @abstractmethod
    def on_checkpoint_complete(self, checkpoint_id):
        """
        :param checkpoint_id:  checkpoint id
        :return:
        """
        raise NotImplementedError("function not implemented!")
