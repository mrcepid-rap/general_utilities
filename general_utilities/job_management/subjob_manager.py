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
    """
    Interface class that launches jobs using the appropriate backend based on the detected platform.
    Supports DNAnexus subjobs (DX) and local threading.
    """

    def __init__(self, concurrent_job_limit: int = 100, retries: int = 1, incrementor: int = 500,
                 log_update_time: int = 60, download_on_complete: bool = False, thread_factor: int = 1,
                 threads: int = None):

        self._logger = MRCLogger(__name__).get_logger()
        self._gcp_check_result = None  # for memoization
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

    def queue_subjob(self, function, inputs: dict, outputs: list = None, name: str = None,
                     instance_type: str = None) -> None:
        """
        Queue a subjob (DX) or a thread (local) using a unified interface.
        """
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
        """
        if self._platform == Platform.DX:
            self._launcher.submit_queue()
        elif self._platform == Platform.LOCAL:
            self._launcher.collect_futures()

    def get_outputs(self) -> list:
        """
        Retrieve outputs of completed jobs.
        """
        return list(self._launcher)

    def _platform_identifier(self) -> Platform:
        """
        Identifies the platform based on the environment and system information.
        """
        if re.match(r'job-\w{24}', self._platform_uname):
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
        """
        if self._gcp_check_result is not None:
            return self._gcp_check_result

        try:
            response = requests.get('http://metadata.google.internal', timeout=0.5)
            self._gcp_check_result = (
                    response.status_code == 200 and
                    response.headers.get('Metadata-Flavor') == 'Google'
            )
            self._logger.info(f"GCP VM detection result: {self._gcp_check_result}")
        except Exception as e:
            self._logger.info(f"GCP VM detection failed")
            self._gcp_check_result = False

        return self._gcp_check_result

