import math
import os
from abc import ABC, abstractmethod
from asyncio import sleep

from general_utilities.mrc_logger import MRCLogger


class JobLauncherInterface(ABC):
    """
    Interface class for launching jobs using different backends.
    """

    def __init__(self, incrementor: int = 500, threads: int = None, error_message="An error occurred"):

        self._logger = MRCLogger(__name__).get_logger()

        self._threads = self._get_threads(threads)
        self._incrementor = incrementor
        self._error_message = error_message

        self._num_completed_jobs = 0
        self._num_jobs = 0
        self._output_array = []

    def __iter__(self):
        return self

    # I have a feeling the sleep() part is VERY inefficient but I am unsure of how to fix at the moment...
    # Due to how futures work, most of the time a next() call will receive a 'None' return. We need to create a
    # waiter that will hold until we get something OTHER than None, so we can return it to the main class.
    def __next__(self):
        curr_future = next(self._output_array)
        while curr_future is None:
            curr_future = next(self._output_array)
            sleep(0.1)
        self._num_completed_jobs += 1
        self._print_status()
        return curr_future.result()

    def _get_threads(self, requested_threads: int):
        """
        Get the number of threads to use for job execution.
        """
        threads = requested_threads if requested_threads else os.cpu_count()
        if threads is None or threads < 1:
            raise ValueError('Not enough threads on machine to complete task. Number of threads on this machine '
                             f'({threads}) is less than 1.')
        return threads

    def _print_status(self):
        """
        Print a time-stamped log of jobs waiting in the queue to be submitted and currently running.
        """
        if math.remainder(self._num_completed_jobs, self._incrementor) == 0 or \
                self._num_completed_jobs == self._num_jobs:
            self._logger.info(
                f'{"Total number of jobs finished":{50}}: {self._num_completed_jobs} / {self._num_jobs} '
                f'({((self._num_completed_jobs / self._num_jobs) * 100):0.2f}%)'
            )
            self._logger.info(f'{"Jobs currently running":{65}}: {self._num_jobs} / {self._num_completed_jobs}')

    @abstractmethod
    def launch_job(self, function, inputs: dict, outputs: list = None, name: str = None,
                   instance_type: str = None) -> None:
        pass

    @abstractmethod
    def submit_queue(self) -> None:
        pass
