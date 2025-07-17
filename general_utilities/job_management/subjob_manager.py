import platform
from abc import ABC, abstractmethod
from enum import Enum
import re

import requests
from general_utilities.job_management.subjob_utility import SubjobUtility
from general_utilities.job_management.thread_utility import ThreadUtility
from general_utilities.mrc_logger import MRCLogger


class Platform(Enum):
    """
    Enum representing the platform that is being used for job management.
    """

    LOCAL = 'Local'
    DX = 'DNANexus'
    GCP = 'Google Cloud Platform'


class JobLauncherInterface(ABC):
    """
    Interface class for launching jobs using different backends.
    This class should be implemented by any job launcher utility.
    """

    @abstractmethod
    def queue_subjob(self, function, inputs: dict, outputs: list = None, name: str = None,
                     instance_type: str = None) -> None:
        """
        Queue a subjob with the given parameters.
        """
        pass

    @abstractmethod
    def submit_and_monitor(self) -> None:
        """
        Submit the queued jobs and monitor their execution.
        """
        pass

    @abstractmethod
    def get_outputs(self) -> list:
        """
        Retrieve outputs of completed jobs.
        """
        pass

    @property
    @abstractmethod
    def platform(self) -> Platform:
        """
        Return the detected platform (DX, LOCAL, or GCP).
        """
        pass


class SubjobLauncher(JobLauncherInterface):
    """
    Concrete implementation of JobLauncherInterface for launching subjobs on DNAnexus.
    """

    def __init__(self, concurrent_job_limit: int = 100, retries: int = 1, incrementor: int = 500,
                 log_update_time: int = 60, download_on_complete: bool = False):
        self._launcher = SubjobUtility(
            concurrent_job_limit=concurrent_job_limit,
            retries=retries,
            incrementor=incrementor,
            log_update_time=log_update_time,
            download_on_complete=download_on_complete
        )

    def queue_subjob(self, function, inputs: dict, outputs: list = None, name: str = None,
                     instance_type: str = None) -> None:
        """
        Queue a subjob with the given parameters.
        :param function: the function to be executed as a subjob
        :param inputs: a dictionary of inputs for the subjob
        :param outputs: a list of outputs for the subjob
        :param name: the name of the subjob
        :param instance_type: the type of instance to run the subjob on (optional)
        :return: None
        """
        self._launcher.launch_job(
            function=function,
            inputs=inputs,
            outputs=outputs,
            name=name,
            instance_type=instance_type
        )

    def submit_and_monitor(self) -> None:
        """
        Submit the queued jobs and monitor their execution.
        :return: None
        """
        self._launcher.submit_queue()

    def get_outputs(self) -> list:
        """
        Retrieve outputs of completed jobs.
        :return: list of outputs from the subjobs
        """
        return list(self._launcher)

    @property
    def platform(self) -> Platform:
        """
        Return the detected platform for this job launcher.
        :return: the platform to run the subjobs on
        """
        return Platform.DX


class ThreadLauncher(JobLauncherInterface):
    """
    Concrete implementation of JobLauncherInterface for launching threads locally.
    """

    def __init__(self, threads: int = None, error_message: str = 'A local thread failed.',
                 incrementor: int = 500, thread_factor: int = 1):
        self._launcher = ThreadUtility(
            threads=threads,
            error_message=error_message,
            incrementor=incrementor,
            thread_factor=thread_factor
        )

    def queue_subjob(self, function, inputs: dict, outputs: list = None, name: str = None,
                     instance_type: str = None) -> None:
        """
        Queue a subjob via the thread launcher with the given parameters.
        :param function: the function to be executed as a subjob
        :param inputs: a dictionary of inputs for the subjob
        :param outputs: a list of outputs for the subjob (optional)
        :param name: the name of the subjob (optional)
        :param instance_type: the type of instance to run the subjob on (optional)
        :return: None
        """
        self._launcher.launch_job(
            class_type=function,
            **inputs
        )

    def submit_and_monitor(self) -> None:
        """
        Submit the queued jobs and monitor their execution.
        :return: None
        """
        self._launcher.collect_futures()

    def get_outputs(self) -> list:
        """
        Retrieve outputs of completed jobs.
        :return: list of outputs from the threads and/or subjobs
        """
        return list(self._launcher)

    @property
    def platform(self) -> Platform:
        """
        Return the detected platform for this job launcher.
        :return: the platform to run the threads on
        """
        return Platform.LOCAL


class JobLauncherFactory:
    """
    Factory class to create job launcher instances based on the detected platform.
    """

    def __init__(self, concurrent_job_limit=100, retries=1, incrementor=500, log_update_time=60,
                 download_on_complete=False, thread_factor=1, threads=None):

        self._logger = MRCLogger(__name__).get_logger()
        self._gcp_check_result = None
        self._platform = self._detect_platform()

        self._logger.info(f"Detected platform: {self._platform.value}")

        self._config = dict(
            concurrent_job_limit=concurrent_job_limit,
            retries=retries,
            incrementor=incrementor,
            log_update_time=log_update_time,
            download_on_complete=download_on_complete,
            thread_factor=thread_factor,
            threads=threads
        )

    def get_launcher(self) -> JobLauncherInterface:
        """
        Get the appropriate job launcher based on the detected platform.
        :return: JobLauncherInterface: an instance of a job launcher (SubjobLauncher or ThreadLauncher)
        """
        if self._platform == Platform.DX:
            return SubjobLauncher(
                concurrent_job_limit=self._config['concurrent_job_limit'],
                retries=self._config['retries'],
                incrementor=self._config['incrementor'],
                log_update_time=self._config['log_update_time'],
                download_on_complete=self._config['download_on_complete']
            )
        elif self._platform == Platform.LOCAL:
            return ThreadLauncher(
                threads=self._config['threads'],
                error_message='A local thread failed.',
                incrementor=self._config['incrementor'],
                thread_factor=self._config['thread_factor']
            )
        else:
            raise RuntimeError("GCP is not supported for job launching.")

    def _detect_platform(self) -> Platform:
        """
        Identifies the platform based on the environment and system information.
        """
        if re.match(r'job-\w{24}', self._detect_platform_uname()):
            return Platform.DX

        if self._is_running_on_gcp_vm():
            return Platform.GCP

        return Platform.LOCAL

    def _detect_platform_uname(self) -> str:
        """
        Detects the platform by checking the system's uname information.
        """
        uname_info = platform.uname().node.lower()
        self._logger.info(f"Platform uname info: {uname_info}")
        return uname_info

    def _is_running_on_gcp_vm(self) -> bool:
        """
        Detects if the script is running on a Google Compute Engine VM.
        Memoized to avoid repeated metadata server queries.
        Returns False silently if detection fails.
        """
        if self._gcp_check_result is not None:
            return self._gcp_check_result

        try:
            response = requests.get('http://metadata.google.internal', timeout=0.5)
            self._gcp_check_result = (
                    response.status_code == 200 and
                    response.headers.get('Metadata-Flavor') == 'Google'
            )
        except Exception:
            self._gcp_check_result = False

        return self._gcp_check_result
