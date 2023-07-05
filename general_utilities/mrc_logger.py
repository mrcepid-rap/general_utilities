import os
import dxpy
import logging

from logging import Logger


class MRCLogger:

    def __init__(self, name_suffix: str = None):
        """A wrapper around the DNA Nexus logging facility

        We implemented this class to ensure consistent output when running via DNANexus. We can instantiate this class
        in two ways:

        1. with a name_suffix, to create a sublogger
        2. Without a name_suffix, which builds the top-level logger which every other logger hooks into when a
            name_suffix is provided.

        :param name_suffix: Name to start the logger with. Typically, will be derived from the __name__ automethod
        """

        if name_suffix is None:
            name = 'MRCLogger'
        else:
            name = '.'.join(['MRCLogger', name_suffix])

        self._logger = logging.getLogger(name)

        if not self._check_previous_handlers():
            if 'DX_JOB_ID' in os.environ:
                self._logger.addHandler(dxpy.DXLogHandler())
            else:
                self._logger.addHandler(logging.StreamHandler())
            self._logger.propagate = False
            self._logger.setLevel(logging.INFO)

    def _check_previous_handlers(self) -> bool:
        """Check to make sure the DXLogHandler has not previously been attached to this logger

        This situation can arise if a class is instantiated multiple times, as the default method is to build the
        logger with the name of the class. So if an instance of class 'Foo' has already been created then the logger
        MRCLogger.Foo already exists. If 'Foo' is built again, then it will try to attach another DXLogHandler to the
        logger causing logs to be printed as many times as 'Foo' has been created.

        :return: boolean indicating if a dxpy.DXLogHandler has already been attached to this logger
        """

        found_handler = False
        for handler in self._logger.handlers:
            if isinstance(handler, dxpy.DXLogHandler) or isinstance(handler, logging.StreamHandler):
                found_handler = True

        return found_handler

    def get_logger(self) -> Logger:
        """Getter for the logger built by this class

        :return: The logger generated by this class
        """

        return self._logger
