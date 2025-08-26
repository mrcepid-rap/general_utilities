import re
from enum import Enum

import dxpy
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
        if self._is_running_on_dnanexus():
            platform_type = Platform.DX

        elif self._is_running_on_gcp_vm():
            platform_type = Platform.GCP

        else:
            platform_type = Platform.LOCAL

        return platform_type

    def _is_running_on_dnanexus(self) -> bool:
        """
        Detects the platform by checking whether we are running a DNA Nexus job.
        """
        jobid_info = dxpy.JOB_ID
        if re.match(r'job-\w{24}', str(jobid_info)):
            dna_nexus_job = True
            self._logger.info(f"Running on DNA Nexus job: {str(jobid_info)}")
        else:
            dna_nexus_job = False
        return dna_nexus_job

    def _is_running_on_gcp_vm(self) -> bool:
        """
        Detects if the script is running on a Google Compute Engine VM.
        Returns False silently if detection fails.
        """
        try:
            response = requests.get('http://metadata.google.internal', timeout=0.5)
            result = (
                    response.status_code == 200 and
                    response.headers.get('Metadata-Flavor') == 'Google'
            )
        except requests.RequestException:
            result = False
        return result