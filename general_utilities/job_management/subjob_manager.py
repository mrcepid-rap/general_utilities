import platform
import re
import socket
from enum import Enum
from socket import socket

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


class SubjobUtilityInterface:
    def __init__(self, concurrent_job_limit: int = 100, retries: int = 1, incrementor: int = 500,
                 log_update_time: int = 60, download_on_complete: bool = False, thread_factor: int = 1,
                 threads: int = None):

        self._logger = MRCLogger(__name__).get_logger()
        self._is_gcp = self._is_running_on_gcp_vm()
        self._platform_uname = self._detect_platform_uname()
        self._platform = self._platform_identifier()

        self._logger.info(f"Detected platform: {self._platform.value}")

        if self._platform == Platform.DX:
            self._launcher = SubjobUtility(
                concurrent_job_limit=concurrent_job_limit,
                retries=retries,
                incrementor=incrementor,
                log_update_time=log_update_time,
                download_on_complete=download_on_complete
            )
        elif self._platform == Platform.LOCAL:
            self._launcher = ThreadUtility(
                threads=threads,
                error_message='A local thread failed.',
                incrementor=incrementor,
                thread_factor=thread_factor
            )
        else:
            raise RuntimeError("GCP not supported for job/thread launching.")

    def queue_subjob(self, function, inputs: dict, outputs: list = None, name: str = None, instance_type: str = None) -> None:
        """
        Launch a subjob (DX) or a thread (local) using a unified interface.

        :param function: The function or class to be executed as a subjob.
        :param inputs: A dictionary of inputs to be passed to the function.
        :param outputs: A list of outputs expected from the function (DX only).
        :param name: The name of the job (DX only).
        :param instance_type: The instance type for the job (DX only).
        :return: None
        """
        # Launch a subjob or thread based on the platform
        if self._platform == Platform.DX:
            self._launcher.launch_job(
                function=function,
                inputs=inputs,
                outputs=outputs,
                name=name,
                instance_type=instance_type
            )
        elif self._platform == Platform.LOCAL:
            self._launcher.launch_job(
                class_type=function,
                **inputs
            )

    def submit_and_monitor(self) -> None:
        """
        Submit the queued jobs and monitor their execution.
        This method is used to finalize the job submission process and wait for the jobs to complete.
        :return: None
        """
        if self._platform == Platform.DX:
            self._launcher.submit_queue()
        elif self._platform == Platform.LOCAL:
            self._launcher.collect_futures()

    def get_outputs(self) -> list:
        """
        Retrieve the outputs of the completed jobs.
        :return: A list of outputs from the completed jobs.
        """
        return list(self._launcher)

    def _platform_identifier(self) -> Platform:
        """
        Identifies the platform based on the environment and system information.
        :return: An instance of Platform enum representing the detected platform.
        """

        if re.match(r'job-\w{24}', self._platform_uname):
            return Platform.DX

        if self._is_running_on_gcp_vm():
            return Platform.GCP

        return Platform.LOCAL

    def _detect_platform_uname(self) -> str:
        """
        Detects the platform by checking the system's uname information.
        :return: A string representing the platform.
        """
        uname_info = platform.uname().node.lower()
        self._logger.info(f"Platform uname info: {uname_info}")
        return uname_info

    def _is_running_on_gcp_vm(self) -> bool:
        """
        Detects if the script is running on a Google Compute Engine VM.
        :return: True if running on a GCP VM, False otherwise.
        """
        try:
            response = requests.get('http://metadata.google.internal', timeout=0.5)
            result = response.status_code == 200 and response.headers.get('Metadata-Flavor') == 'Google'
            self._logger.info(f"GCP VM detection result: {result}")
            return result
        except (requests.exceptions.RequestException, socket.timeout):
            self._logger.info("GCP VM detection failed due to request exception.")
            return False
