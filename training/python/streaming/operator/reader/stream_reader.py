from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

from .reader import Reader


class StreamReader(Reader):
    """
    Stream Data Reader
    """

    def __init__(self, start_offset=0):
        super().__init__()
        self._start_offset = start_offset

    def get_offset(self):
        raise NotImplementedError("function not implemented")

    def fetch_data(self, batch_size, **options):
        raise NotImplementedError("function not implemented")

    def seek_by_offset(self, start_offset):
        raise NotImplementedError("function not implemented")

    def clear_expired_data(self, expired_offset):
        raise NotImplementedError("function not implemented")
