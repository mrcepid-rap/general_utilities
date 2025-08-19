import re
from enum import Enum

import requests

from general_utilities.mrc_logger import MRCLogger


class Platform(Enum):
    """
    Enum representing the platform that is being used for job management.
    """

    LOCAL = 'Local'
    DX = 'DNANexus'
    GCP = 'Google Cloud Platform'


class PlatformFactory:
    """
    Factory class to create job launcher instances based on the detected platform.
    This class detects the platform (DX, GCP, or LOCAL) and returns an appropriate job launcher instance.
    """

    def __init__(self):

        self._logger = MRCLogger(__name__).get_logger()

        self._gcp_check_result = None
        self._platform = self._detect_platform()

    def get_platform(self) -> Platform:
        """
        Returns the detected platform.
        """
        return self._platform

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
