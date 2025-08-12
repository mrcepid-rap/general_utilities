from typing import Iterator, Union

from general_utilities.job_management.subjob_utility import SubjobUtility
from general_utilities.job_management.thread_utility import ThreadUtility
from general_utilities.mrc_logger import MRCLogger
from general_utilities.platform_utils.platform_factory import Platform, PlatformFactory


class JobLauncher:
    """
    Job launcher that uses either SubjobUtility for DX or ThreadUtility for local execution.
    GCP is not supported in this implementation.

    This class detects the platform and initializes the appropriate backend utility for job management.
    It provides methods to launch jobs, submit and monitor them, and retrieve outputs.

    :param incrementor: The incrementor for job submission, default is 500.
    :param threads: The number of threads to use for local execution, default is None.
    :raises NotImplementedError: If GCP platform is detected.
    :raises RuntimeError: If an unsupported platform is detected.
    """

    def __init__(self, **kwargs):
        self._logger = MRCLogger(__name__).get_logger()

        self._platform = PlatformFactory().get_platform()
        self._logger.info(f"Detected platform: {self._platform.value}")
        self._kwargs = kwargs
        self._backend = None

    def _get_backend(self) -> Union[SubjobUtility, ThreadUtility]:
        """
        Get the backend utility for job management based on the detected platform.
        :return: An instance of SubjobUtility for DX or ThreadUtility for local execution.
        """
        if self._backend is None:
            if self._platform == Platform.DX:
                self._backend = SubjobUtility(**self._kwargs)
            elif self._platform == Platform.LOCAL:
                self._backend = ThreadUtility(**self._kwargs)
            else:
                raise RuntimeError("Unsupported platform")
        return self._backend

    def launch_job(self, *args, **kwargs) -> None:
        """
        Queue a subjob with the given parameters.
        :param args: Positional arguments for the job function.
        :param kwargs: Keyword arguments for the job function, including inputs, outputs, name, and instance_type.
        :return: None
        """
        self._get_backend().launch_job(*args, **kwargs)

    def submit_and_monitor(self) -> None:
        """
        Submit the queued jobs and monitor their execution.
        :return: None
        """
        self._get_backend().submit_and_monitor()

    def get_outputs(self) -> Iterator:
        """
        Retrieve outputs of completed jobs.
        :return: An iterator over the outputs from the subjobs.
        """
        return iter(self._get_backend())

    @property
    def platform(self) -> Platform:
        """
        Return the detected platform for this job launcher.
        :return: The platform to run the subjobs on.
        """
        return self._platform
