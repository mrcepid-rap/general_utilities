import platform
import re
from enum import Enum
from pathlib import Path
from typing import List, Dict, Union

import dxpy
import requests
from dxpy import DXFile

from general_utilities.import_utils.file_handlers.dnanexus_utilities import generate_linked_dx_file
from general_utilities.import_utils.file_handlers.input_file_handler import InputFileHandler
from general_utilities.mrc_logger import MRCLogger


class Platform(Enum):
    """
    Enum representing the platform that is being used for job management.
    """

    LOCAL = 'Local'
    DX = 'DNANexus'
    GCP = 'Google Cloud Platform'


class ExportFileHandler:
    """
    This class is designed to recognize the platform on which it is running (Local, DNANexus, or GCP) and then
    upload files accordingly.

    For DNA Nexus, it converts the input files to DX links using the `dxpy.dxlink` function, and returns a
    dictionary or list of dictionaries containing the DX links.

    For GCP, the upload logic is not implemented yet, but it can be extended in the future.

    For Local, it simply returns an empty dictionary, as no upload is performed.
    """

    def __init__(self):

        self._logger = MRCLogger(__name__).get_logger()
        self._gcp_check_result = None
        self.platform = self._detect_platform()

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

    def _convert_file_to_dxlink(self, file: Union[str, Path, DXFile, dict]) -> dict:
        """
        Converts a file input to a DX link.
        This method handles different types of inputs robustly by using the InputFileHandler class.

        :param file: The input file to be converted to a DX link.
        :return: A DX link dictionary for the file.
        """
        # Treat everything else via InputFileHandler
        handler = InputFileHandler(file)
        handle = handler.get_file_handle()
        return dxpy.dxlink(generate_linked_dx_file(handle))

    def export_files(self, files_input: Union[str, Path, dict, List[Union[str, Path, dict]], Dict[
        str, Union[str, Path, dict, List[Union[str, Path, dict]]]]]) -> Union[
        dict, List[dict], Dict[str, Union[dict, List[dict]]], str, Path, DXFile]:
        """
        Export files according to platform

        This method handles the export of files based on the detected platform:
        - For Local platform, it does not perform any upload and returns an empty dictionary.
        - For GCP platform, it currently logs a message that upload logic is not implemented.
        - For DNANexus platform, it converts the input files to DX links using the `_convert_file_to_dxlink` method.

        :param files_input: The input files to be exported. Can be a single file path, a list of file paths,
            a dictionary with file paths, or a list of dictionaries.
        :return: A dictionary or list of dictionaries containing the DX links for the files.
        """
        # Check if if the platform is Local, in which case no upload is performed
        if self.platform == Platform.LOCAL:
            if self._logger:
                self._logger.info("Local platform detected: no upload performed.")
            return {}

        # Check if the platform is GCP, in which case upload logic is not implemented yet
        if self.platform == Platform.GCP:
            if self._logger:
                self._logger.info("GCP platform detected: upload logic not implemented yet.")
            return {}

        # Check if the platform is DNANexus, in which case we convert files to DX links
        # Note that the input can be a single file, a list of files, or a dictionary of files
        # The output will be a DX link or a list/dictionary of DX links, so that it can be used in DNANexus jobs.
        if self.platform == Platform.DX:
            # If the input is a single file, convert it to a DX link
            if isinstance(files_input, (str, Path)):
                return self._convert_file_to_dxlink(files_input)

            # If the input is a list of files, convert each file to a DX link
            if isinstance(files_input, list):
                return [self._convert_file_to_dxlink(f) for f in files_input]

            # If the input is a dictionary, convert each file to a DX link and return a dictionary
            if isinstance(files_input, dict):
                output = {}
                for k, v in files_input.items():
                    if isinstance(v, list):
                        output[k] = [self._convert_file_to_dxlink(f) for f in v]
                    else:
                        output[k] = self._convert_file_to_dxlink(v)
                return output

            # If the input is not a recognized type, raise an error
            raise TypeError(f"Unsupported input type for export_files: {type(files_input)}")

        else:
            # If the platform is not recognized, log an error
            if self._logger:
                self._logger.error(f"Unsupported platform: {self.platform}")
