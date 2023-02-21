import dxpy
import logging


class MRCLogger:

    # Instantiate w/o name_suffix from the 'main' class of the applet, but include a name for sublcasses via __name__
    # auto-method
    def __init__(self, name_suffix=None):

        if name_suffix is None:
            self._logger = logging.getLogger('MRCLogger')
        else:
            self._logger = logging.getLogger('.'.join(['MRCLogger', name_suffix]))

        self._logger.addHandler(dxpy.DXLogHandler())
        self._logger.propagate = False
        self._logger.setLevel(logging.INFO)

    def get_logger(self):

        return self._logger
