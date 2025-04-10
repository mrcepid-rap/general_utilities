import re
import subprocess
from enum import Enum
from pathlib import Path
from typing import Union, Optional

import dxpy

from general_utilities.association_resources import download_dxfile_by_name
from general_utilities.mrc_logger import MRCLogger


class FileType(Enum):
    DNA_NEXUS_FILE = "DNA Nexus File"
    LOCAL_PATH = "Local Path"
    GCLOUD_FILE = "Google Cloud File"
    NONE = "None"


class InsmedInput:
    """
    This class is designed to deal with all the different input filetypes that we deal with across multiple platforms.
    For example, if running on DNA Nexus then we are dealing with dxpy files, if running local test, then we are
    dealing with filepaths. Should we work with new platforms, we can add new filetype handling in this class.

    The class takes a fileID of any kind as input, and downloads the file to our local directory. If the output
    filepath is specific, then the file will be downloaded corresponding to that filepath. If the file already exists,
    it will not be downloaded.

    The file_handle will return the local filepath of the file in question.
    """

    def __init__(self, input_str: Union[str, Path, dxpy.DXFile], download_now: bool = False, destination: Path = None):

        # Initiate the InsmedInput class

        # For logging
        self._logger = MRCLogger(__name__).get_logger()

        # set the input string
        self._input_str = input_str

        # Whether the input also specifies where we want to download this file to
        self._destination = destination

        # The file type of the input string
        self._file_type = self._decide_filetype()

        # set the downloader to be false
        self._downloaded = False

        # ACTIONS:

        # Let's get filetype that we are working with as a public attribute
        self.file_type = self._resolve_file_type()

        # if we are downloading now, then we need to download the file
        if download_now:
            self._downloaded = True
            self.file_handle = self._parse_file(self._file_type)
            self._logger.info(f"File downloaded: {self.file_handle}")

    def _resolve_file_type(self) -> Optional[FileType]:
        """
        Resolve the filetype based on the current state of the object.

        This method determines the type of the input file and returns its classification
        as a `FileType` Enum. It is primarily used when the file has not been downloaded yet
        (i.e., `self._downloaded` is `False`).

        If the file is not downloaded, the method uses the `_decide_filetype` function to classify
        the input as one of the supported file types (e.g., DNA Nexus file, local path, or
        Google Cloud file).

        :return:
            `FileType`: Enum value representing the type of the input file (e.g., DNA Nexus file, local path).

        :raises FileNotFoundError: If the input file cannot be resolved or is invalid.
        """
        # if we are not downloading now but we want to know the filetype
        if not self._downloaded:
            return self._file_type

    def _parse_file(self, file_type: FileType) -> Path:
        """
        Download a file based on its type using an Enum.

        This method determines the appropriate download function to call based on the provided file type.
        It uses a mapping of `FileType` Enum values to corresponding methods for handling different file types.
        If the file type is unsupported, it raises a `ValueError`.

        :param file_type: A `FileType` Enum value representing the type of the file to be downloaded.
        :return: A `Path` object representing the local file path of the downloaded file.
        :raises ValueError: If the provided file type is not supported.
        """
        # Map Enum values to corresponding methods
        method_map = {
            FileType.DNA_NEXUS_FILE: self._download_dnanexus_file,
            FileType.LOCAL_PATH: self._resolve_local_file,
            FileType.GCLOUD_FILE: self._download_gsutil_file,
        }

        if file_type in method_map:
            return method_map[file_type]()
        else:
            raise ValueError(f"Unsupported file type: {file_type}")

    def _download_dnanexus_file(self) -> Path:
        """
        Download a file from DNA Nexus.

        This method downloads a file from DNA Nexus using the provided file ID or name. If a destination path is specified
        and already exists, it returns the destination path without downloading. Otherwise, it downloads the file to the
        specified destination or the current working directory.

        :return: A `Path` object representing the resolved local file path of the downloaded file.
        :raises dxpy.exceptions.DXError: If the DNA Nexus file download fails.
        """
        if self._destination is None:
            file_path = download_dxfile_by_name(self._input_str)
            return Path(file_path)
        elif self._destination.exists():
            # If the destination already exists
            return self._destination
        else:
            # Download file to destination
            file_path = dxpy.download_dxfile(self._input_str, str(self._destination))
            return Path(file_path)

    def _resolve_local_file(self) -> Path:
        """
        Resolve a local file path.

        This method checks if the provided input string corresponds to an existing local file. If the file exists,
        it returns the resolved absolute path. If a destination path is specified and exists, it returns the destination
        path. If neither condition is met, it raises a `FileNotFoundError`.

        :return: A `Path` object representing the resolved local file path.
        :raises FileNotFoundError: If the file does not exist locally or the destination path is not valid.
        """
        path = Path(self._input_str)
        if path.exists() and path.is_file():
            return path.resolve()
        if self._destination and self._destination.exists():
            return self._destination
        raise FileNotFoundError(f"Local file not found: {self._input_str}")

    def _download_gsutil_file(self) -> Path:
        """
        Download a file from a Google Cloud Storage (GCS) bucket using the `gsutil` command-line tool.

        This method uses the `gsutil cp` command to copy a file from a GCS bucket to the current working directory.
        The input file path must be a valid GCS URI (e.g., `gs://bucket-name/file-name`). The method logs the result
        of the download operation and returns the resolved local file path.

        :return: A `Path` object representing the resolved local file path of the downloaded file.
        :raises subprocess.CalledProcessError: If the `gsutil` command fails during execution.
        """
        output_path = self._destination or Path(self._input_str).name
        gsutil_cmd = f"gsutil cp {self._input_str} {output_path}"
        result = subprocess.run(gsutil_cmd, shell=True, check=True, text=True, capture_output=True)
        self._logger.info(f"Downloaded file using gsutil: {result.stdout.strip()}")
        return Path(self._input_str).resolve()

    def _decide_filetype(self) -> Union[FileType]:
        """
        Determine the type of the input and classify it as a DNA Nexus file, a local path, or an existing file object.

        This method evaluates the input string to identify whether it represents a DNA Nexus file ID, a local file path,
        or an existing `DXFile` or `Path` object. It handles the following scenarios:
        - If the input is `None` or the string 'None', it raises a `FileNotFoundError`.
        - If the input is already a `DXFile` or `Path` object, it returns the corresponding file type.
        - If the input is a local path, it ensures the path is absolute and checks if it exists.
        - If the input matches the format of a DNA Nexus file ID, it validates the ID and returns the corresponding file type.
        - If the input is a file name, it attempts to locate the file on DNA Nexus and returns the corresponding `DXFile` object.

        :return: A `FileType` Enum value representing the type of the input, or a `DXFile` object if found on DNA Nexus.
        :raises FileNotFoundError: If the input is `None`, or the file is not found locally or on DNA Nexus.
        :raises ValueError: If the local path is not absolute.
        :raises dxpy.exceptions.DXError: If the DNA Nexus file ID is invalid or cannot be described.
        """
        if self._input_str is None or self._input_str == 'None':
            return FileType.NONE

        # Handle existing DXFile or Path objects
        if isinstance(self._input_str, (dxpy.DXFile, Path)):
            return FileType.DNA_NEXUS_FILE if isinstance(self._input_str, dxpy.DXFile) else FileType.LOCAL_PATH

        # Handle local path first
        path = Path(self._input_str)
        if not path.is_absolute():
            path = path.resolve()

        if path.exists():
            return FileType.LOCAL_PATH

        # Check if it's a DNA Nexus file ID
        if re.fullmatch(r'file-\w{24}', self._input_str):
            try:
                dx_file = dxpy.DXFile(dxid=self._input_str)
                dx_file.describe()
                return FileType.DNA_NEXUS_FILE
            except dxpy.exceptions.DXError as e:
                self._logger.error(f"Invalid DNA Nexus file ID: {e}")
                raise

        # Try to find on DNA Nexus by path
        try:
            found = dxpy.find_one_data_object(
                classname='file',
                project=dxpy.PROJECT_CONTEXT_ID,
                name_mode='exact',
                name=path.name,
                folder=str(path.parent),
                zero_ok=False
            )
            return FileType.DNA_NEXUS_FILE
        except dxpy.exceptions.DXSearchError:
            raise FileNotFoundError(f"File not found locally or on DNA Nexus: {self._input_str}")

    def download(self, destination: Optional[Path] = None) -> Path:
        """
        Public method to trigger the download of the file.

        Args:
            destination (Optional[Path]): An optional override for the destination path.

        Returns:
            Path: The path to the downloaded file.
        """
        if destination:
            self._destination = destination

        if not self._downloaded:
            self._logger.info("Starting file download...")
            self._downloaded = True
            self.file_handle = self._parse_file(self._file_type)
            self._logger.info(f"File downloaded successfully: {self.file_handle}")
            return self.file_handle
        else:
            self._logger.info(f"File was already downloaded: {self.file_handle}")
            return self.file_handle