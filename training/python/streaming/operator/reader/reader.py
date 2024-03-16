from abc import ABCMeta, abstractmethod


class Reader(metaclass=ABCMeta):
    """
    Data Reader
    """

    def __init__(self):
        super(Reader, self).__init__()

    def __str__(self):
        print("This is StreamReader.")

    @abstractmethod
    def fetch_data(self, batch_size, **options):
        """
        fetch train data which has batch_size records each
        Args:
            batch_size: number of records required
        Returns:
            list: which has batch_size records each
        """

    @abstractmethod
    def seek_by_offset(self, start_offset):
        """
        seek data by specified start offset
        :param start_offset:
        :return:
        """

    @abstractmethod
    def get_offset(self):
        """
        Get current offset
        :return:  current offset
        """
