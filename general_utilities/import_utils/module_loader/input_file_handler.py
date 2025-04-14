import re
from enum import Enum, auto
from pathlib import Path
from typing import Union

import dxpy
from google.cloud import storage

from general_utilities.association_resources import download_dxfile_by_name
from general_utilities.mrc_logger import MRCLogger


class FileType(Enum):
    """Enum representing different file types."""
    DNA_NEXUS_FILE = auto()
    LOCAL_PATH = auto()
    GCLOUD_FILE = auto()


class InputFileHandler:
    """
    This class is designed to deal with all the different input filetypes that we deal with across multiple platforms.
    For example, if running on DNA Nexus then we are dealing with dxpy files, if running local test, then we are
    dealing with filepaths. Should we work with new platforms, we can add new filetype handling in this class.

    The class takes a fileID of any kind as input, and downloads the file to our local directory. If the output
    filepath is specific, then the file will be downloaded corresponding to that filepath. If the file already exists,
    it will not be downloaded.

    The file_handle will return the local filepath of the file in question.
    """

    def __init__(self, input_file: Union[str, Path, dxpy.DXFile], download_now: bool = False):
        """
        Initializes the InsmedInput class to handle input files from different platforms.

        :param input_file: The input file identifier (e.g., a DXFile object, local path, or GCS URI).
        :param download_now: If True, download the file during initialization.
        """

        # Initiate the InsmedInput class
        # For logging
        self._logger = MRCLogger(__name__).get_logger()

        # set the input string
        self._input_str = input_file

        # The file type of the input string
        self._file_type = self._decide_filetype()

        # set the downloader to be false
        self._downloaded = False

        # ACTIONS:
        # if we are downloading now, then we need to download the file
        if download_now:
            self.file_handle = self.get_file_handle()
            self._downloaded = True
            self._logger.debug(f"File downloaded: {self.file_handle}")
        else:
            self.file_handle = None

    def get_file_type(self) -> FileType:
        """
        Return the resolved file type for the input.

        :return: A `FileType` enum indicating the input file type.
        """
        # if we are not downloading now, but we want to know the filetype
        return self._file_type

    def get_input_str(self) -> Union[str, Path, dxpy.DXFile]:
        """
        Retrieve the input string or object provided during initialization.

        This method returns the original input string or object (e.g., file ID, local path, or `DXFile`)
        that was passed to the `InsmedInput` class during its instantiation.

        :return: The input string or object (`str`, `Path`, or `dxpy.DXFile`) provided to the class.
        """
        return self._input_str

    def _resolve_file(self, file_type: FileType) -> Path:
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
            FileType.DNA_NEXUS_FILE: self._resolve_dnanexus_file,
            FileType.LOCAL_PATH: self._resolve_local_file,
            FileType.GCLOUD_FILE: self._resolve_gsutil_file,
        }

        if file_type in method_map:
            return method_map[file_type]()
        else:
            raise ValueError(f"Unsupported file type: {file_type}")

    def _resolve_dnanexus_file(self) -> Path:
        """
        Download a file from DNA Nexus.

        This method downloads a file from DNA Nexus using the provided file ID or name. The file will be downloaded
        to the current working directory.

        :return: A `Path` object representing the resolved local file path of the downloaded file.
        :raises dxpy.exceptions.DXError: If the DNA Nexus file download fails.
        """
        file_path = download_dxfile_by_name(self._input_str)
        return Path(file_path).resolve()

    def _resolve_local_file(self) -> Path:
        """
        Resolve a local file path.

        This method checks if the provided input string corresponds to an existing local file. If the file exists,
        it returns the resolved absolute path. If not, it raises a `FileNotFoundError`.

        :return: A `Path` object representing the resolved local file path.
        :raises FileNotFoundError: If the file does not exist locally.
        """
        path = Path(self._input_str)
        if path.exists() and path.is_file():
            return path.resolve()
        else:
            raise FileNotFoundError(f"Local file not found: {self._input_str}")

    def _resolve_gsutil_file(self) -> Path:
        """
        Download a file from a Google Cloud Storage (GCS) bucket using the Google Cloud Storage Python client.

        NOTE: google-cloud-storage must be configured and authenticated for this to work.

        The input file path must be a valid GCS URI (e.g., `gs://bucket-name/file-name`). The method downloads
        the file to the current working directory.

        :return: A `Path` object representing the resolved local file path of the downloaded file.
        :raises FileNotFoundError: If the GCS file path is invalid or download fails.
        """
        ##### TO-DO
        # this sections needs to be properly tested on GCloud

        # Parse GCS URI
        match = re.match(r'^gs://([^/]+)/(.+)$', self._input_str)
        if not match:
            raise ValueError(f"Invalid GCS path: {self._input_str}")

        bucket_name, blob_name = match.groups()
        output_path = Path(blob_name).name
        output_path = Path(output_path)

        if output_path.exists():
            return output_path.resolve()

        # Download the blob
        try:
            client = storage.Client()
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(blob_name)

            blob.download_to_filename(str(output_path))
            self._logger.debug(f"Downloaded file using GCS API: {output_path}")
            return output_path.resolve()

        except Exception as e:
            self._logger.error(f"Failed to download from GCS: {e}")
            raise FileNotFoundError(f"Failed to download {self._input_str}")

    def _decide_filetype(self) -> FileType:
        """
        Determine the type of the input and classify it as a DNA Nexus file, a local path, or an existing file object.

        This method evaluates the input string to identify whether it represents a DNA Nexus file ID, a local file path,
        or an existing `DXFile` or `Path` object. It handles the following scenarios:
        - If the input is `None` or the string 'None', it raises a `ValueError`.
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
            raise ValueError("No input provided, please check")

        # Handle DXFile or Path objects
        elif isinstance(self._input_str, (dxpy.DXFile, Path)):
            return FileType.DNA_NEXUS_FILE if isinstance(self._input_str, dxpy.DXFile) else FileType.LOCAL_PATH

        # Check if the input is a GCloud file
        elif re.match(r'^gs://([^/]+)/(.+)$', self._input_str):
            return FileType.GCLOUD_FILE

        # We may have a path that is written as a string
        elif isinstance(self._input_str, str):
            path = Path(self._input_str)
            if not path.is_absolute():
                path = path.resolve()
                if path.exists():
                    return FileType.LOCAL_PATH

        # Check if it's a DNA Nexus file ID
        elif re.match('file-\\w{24}', self._input_str):
            try:
                dxpy.DXFile(dxid=self._input_str)
                return FileType.DNA_NEXUS_FILE
            except dxpy.exceptions.DXError:
                raise TypeError(f'The input for parameter – {self._input_str} – '
                                f'does not look like a valid DNANexus file ID.')
        else:
            try:
                file_handle = Path(self._input_str)
                dxpy.find_one_data_object(
                    classname='file',
                    project=dxpy.PROJECT_CONTEXT_ID,
                    name_mode='exact',
                    name=f'{file_handle.name}',
                    folder=f'{file_handle.parent}',
                    zero_ok=False
                )
                return FileType.DNA_NEXUS_FILE
            except dxpy.exceptions.DXSearchError:
                raise FileNotFoundError(
                    f'The input parameter – {self._input_str} – was tried as a filepath, but was not found '
                    f'in the project this applet has been executed from. Please confirm the file '
                    f'exists in this project or use a DNANexus file ID (like: file-12345...).')
            except dxpy.exceptions.ResourceNotFound:
                raise TypeError(f'The input for parameter – {self._input_str} – '
                                f'does not exist on the DNANexus platform.')

        raise ValueError(f"Unable to determine file type for input: {self._input_str}")

    def get_file_handle(self, overwrite: bool = False) -> Path:
        """
        Download the file based on its type and return the local file path.

        This method determines the type of the input file (e.g., DNA Nexus file, local path, or Google Cloud file) and downloads it
        to the current working directory. If the file has already been downloaded, it returns the path to the
        existing file without re-downloading.

        Returns:
            Path: The local file path where the file has been downloaded or already exists.

        Raises:
            FileNotFoundError: If the file cannot be resolved or downloaded.
            ValueError: If the file type is unsupported.
            dxpy.exceptions.DXError: If the DNA Nexus file download fails.
        """

        if not self._downloaded:
            self._logger.debug("Starting file download...")
            self.file_handle = self._resolve_file(self._file_type)
            self._downloaded = True
            self._logger.debug(f"File downloaded successfully: {self.file_handle}")
            return self.file_handle
        elif overwrite:
            self._logger.warning("Overwriting existing file...")
            self.file_handle = self._resolve_file(self._file_type)
            self._downloaded = True
            self._logger.info(f"File overwritten successfully: {self.file_handle}")
            return self.file_handle
        else:
            self._logger.warning(f"File was already downloaded: {self.file_handle}")
            return self.file_handle
