import platform
import re
from enum import Enum
from pathlib import Path
from typing import List, Dict, Union

import dxpy
import requests
from dxpy import DXFile

from general_utilities.import_utils.file_handlers.dnanexus_utilities import generate_linked_dx_file
from general_utilities.import_utils.file_handlers.input_file_handler import InputFileHandler, FileType
from general_utilities.mrc_logger import MRCLogger


class Platform(Enum):
    """
    Enum representing the platform that is being used for job management.
    """

    LOCAL = 'Local'
    DX = 'DNANexus'
    GCP = 'Google Cloud Platform'


class ExportFileHandler:
    def __init__(self):
        """Initialize the ExportFileHandler"""
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
        # If it's already a DXFile, just wrap it
        if isinstance(file, DXFile):
            return dxpy.dxlink(file)

        # If it's a dictionary, we expect 'file' to be a key
        if isinstance(file, dict):
            if 'file' not in file:
                raise ValueError("Expected a key 'file' in the dict to specify the path to the file")
            return dxpy.dxlink(generate_linked_dx_file(**file))

        # For str/Path, use InputFileHandler to check type and upload
        handler = InputFileHandler(file)
        if handler.get_file_type() == FileType.DNA_NEXUS_FILE:
            return dxpy.dxlink(generate_linked_dx_file(handler.get_file_handle()))

        return dxpy.dxlink(generate_linked_dx_file(handler.get_file_handle()))

    def export_files(self, files_input: Union[str, Path, dict, List[Union[str, Path, dict]], Dict[
        str, Union[str, Path, dict, List[Union[str, Path, dict]]]]]) -> Union[
        dict, List[dict], Dict[str, Union[dict, List[dict]]], str, Path, DXFile]:
        """
        Export files according to platform
        """
        if self.platform == Platform.LOCAL:
            if self._logger:
                self._logger.info("Local platform detected: no upload performed.")
            return {}

        if self.platform == Platform.GCP:
            if self._logger:
                self._logger.info("GCP platform detected: upload logic not implemented yet.")
            return {}

        # Platform.DX
        if self.platform == Platform.DX:
            if isinstance(files_input, (str, Path)):
                return self._convert_file_to_dxlink(files_input)

            if isinstance(files_input, list):
                return [self._convert_file_to_dxlink(f) for f in files_input]

            if isinstance(files_input, dict):
                output = {}
                for k, v in files_input.items():
                    if isinstance(v, list):
                        output[k] = [self._convert_file_to_dxlink(f) for f in v]
                    else:
                        output[k] = self._convert_file_to_dxlink(v)
                return output

            raise TypeError(f"Unsupported input type for export_files: {type(files_input)}")

        else:
            if self._logger:
                self._logger.error(f"Unsupported platform: {self.platform}")
