import re
from pathlib import Path
from typing import List, Union

import dxpy
from dxpy import DXFile

from general_utilities.import_utils.file_handlers.dnanexus_utilities import generate_linked_dx_file
from general_utilities.mrc_logger import MRCLogger
from general_utilities.platform_utils.platform_factory import PlatformFactory, Platform


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

        self._platform = PlatformFactory().get_platform()
        self._gcp_check_result = None

    def _convert_file_to_dxlink(self, file: Union[str, Path, DXFile, dict]) -> dict:
        """
        Converts a file input to a DX link.
        This method handles different types of inputs robustly by using the InputFileHandler class.

        :param file: The input file to be converted to a DX link.
        :return: A DX link dictionary for the file.
        """

        if isinstance(file, dxpy.DXFile):
            converted_file = file
        elif isinstance(file, dict) and dxpy.is_dxlink(file):
            converted_file = file
        else:
            converted_file = dxpy.dxlink(generate_linked_dx_file(file))
        return converted_file

    def export_files(self, files_input: Union[str, Path, List[Union[str, Path]]]) -> Union[
        Union[str, Path], List[Union[str, Path]], List[dict], dict]:
        """
        Export files according to platform

        This method handles the export of files based on the detected platform:
        - For Local platform, it does not perform any upload and returns an empty dictionary.
        - For GCP platform, it currently logs a message that upload logic is not implemented.
        - For DNANexus platform, it converts the input files to DX links using the `_convert_file_to_dxlink` method.

        :param files_input: A single file (Path or str), or list of files.
        :return: Raw file path(s), or DX link(s) depending on platform.
        """

        result = []

        # Check if the platform is Local, in which case no upload is performed
        if self._platform == Platform.LOCAL:
            self._logger.info("Local platform detected: returning raw file paths.")
            result = files_input if isinstance(files_input, list) else [files_input]

        # Check if the platform is GCP, in which case upload logic is not implemented yet
        elif self._platform == Platform.GCP:
            self._logger.info("GCP platform detected: upload logic not implemented yet.")
            result = []

        # Check if the platform is DNANexus, in which case we convert files to DX links
        # Note that the input can be a single file, a list of files, or a dictionary of files
        # The output will be a DX link or a list/dictionary of DX links, so that it can be used in DNANexus jobs.
        elif self._platform == Platform.DX:
            if isinstance(files_input, list):
                result = [self._convert_file_to_dxlink(f) for f in files_input]
            else:
                result = self._convert_file_to_dxlink(files_input)

        else:
            # If the above didn't work then we have an error somewhere
            self._logger.error(f"Unsupported input type for export_files: {type(files_input)}")

        return result
