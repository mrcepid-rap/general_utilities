import dxpy
import logging


class MRCLogger:

    def __init__(self):

        self._logger = logging.getLogger('MRCLogger')
        self._logger.addHandler(dxpy.DXLogHandler())
        self._logger.propagate = False
        self._logger.setLevel(logging.INFO)

    def get_logger(self):

        return self._logger
