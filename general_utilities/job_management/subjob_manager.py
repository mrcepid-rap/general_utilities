import math
import os
from abc import ABC, abstractmethod
from asyncio import sleep

from general_utilities.job_management.subjob_utility import SubjobUtility
from general_utilities.job_management.thread_utility import ThreadUtility
from general_utilities.mrc_logger import MRCLogger
from general_utilities.platform_utils.platform_factory import PlatformFactory, Platform


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


class JobLauncherFactory:
    """
    Factory class to create job launcher instances based on the detected platform.
    """

    def __init__(self, **kwargs):

        self._logger = MRCLogger(__name__).get_logger()

        self._platform = PlatformFactory().get_platform()
        self._logger.info(f"Detected platform: {self._platform.value}")

        self._launcher = JobLauncher(platform=self._platform, **kwargs)

    @property
    def launcher(self) -> JobLauncherInterface:
        """
        Get the job launcher instance based on the detected platform.
        """
        return self._launcher


class JobLauncher(JobLauncherInterface):
    """
    Job launcher that uses either SubjobUtility for DX or ThreadUtility for local execution.
    GCP is not supported in this implementation.
    """

    def __init__(self, platform: Platform, **kwargs):

        super().__init__(incrementor=kwargs.get('incrementor', 500),
                         threads=kwargs.get('threads'))

        if platform == Platform.DX:
            self._backend = SubjobUtility(**kwargs)
            self._platform = Platform.DX
        elif platform == Platform.LOCAL:
            self._backend = ThreadUtility(**kwargs)
            self._platform = Platform.LOCAL
        elif platform == Platform.GCP:
            raise NotImplementedError("GCP platform is not supported in this implementation.")
        else:
            raise RuntimeError("Unsupported platform, please seek help")

    def launch_job(self, function, inputs, outputs=None, name=None, instance_type=None):
        """
        Launch a job using the appropriate backend based on the platform.
        """
        if self._platform == Platform.DX:
            self._backend.launch_job(function=function, inputs=inputs, outputs=outputs,
                                     name=name, instance_type=instance_type)
        elif self._platform == Platform.LOCAL:
            self._backend.launch_job(function=function, **inputs)
        elif self._platform == Platform.GCP:
            raise NotImplementedError("GCP platform is not supported in this implementation.")
        else:
            raise RuntimeError("Unsupported platform, please seek help")

    def submit_queue(self):
        """
        Submit the queued jobs to the backend for execution.
        """
        self._backend.submit_queue()

    @property
    def platform(self):
        """
        Return the detected platform for this job launcher.
        """
        return self._platform
