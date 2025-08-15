from typing import Union

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

        # Build the backend once
        if self._platform == Platform.DX:
            self._backend = SubjobUtility(**self._kwargs)
        elif self._platform == Platform.LOCAL:
            self._backend = ThreadUtility(**self._kwargs)
        else:
            raise RuntimeError("Unsupported platform")

    def get_backend(self) -> Union[SubjobUtility, ThreadUtility]:
        """
        Get the backend utility for job management.
        :return: The backend utility instance.
        """
        return self._backend

    @property
    def platform(self) -> Platform:
        """
        Return the detected platform for this job launcher.
        :return: The platform to run the subjobs on.
        """
        return self._platform
