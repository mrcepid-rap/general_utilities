import re
import subprocess
from pathlib import Path
from typing import Union, Optional

import dxpy

from general_utilities.association_resources import download_dxfile_by_name
from general_utilities.mrc_logger import MRCLogger


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

    def __init__(self, input_str: Union[str, Path, dxpy.DXFile], download_now: bool, destination: Path = None):

        # Initiate the InsmedInput class

        # For logging
        self._logger = MRCLogger(__name__).get_logger()

        # The input string which can be a path, or a DNA Nexus file-ID, or something else
        self.input_str = input_str

        # Whether the input also specifies where we want to download this file to
        self._destination = destination

        # Let's first see what filetype we are working with
        self.file_handle = self._decide_filetype()

        # if we need the files downloaded, run the downloader
        if download_now:
            self.file_handle = self._download_file()

    def _download_file(self) -> Path:
        """
        Download a file to the specified destination or handle it based on its type.

        This method supports the following scenarios:
        - Downloads DNA Nexus files using the `download_dxfile_by_name` utility.
        - Resolves and returns local file paths if the file already exists.
        - Downloads files using the `gsutil cp` command for Google Cloud Storage paths (in development).

        Behavior:
        - If the input is a DNA Nexus file ID, the file is downloaded to the specified destination or the current directory.
        - If the input is a local file path, it verifies the file's existence and resolves its absolute path.
        - If the input is a Google Cloud Storage path, the file is downloaded using `gsutil`.

        Returns:
            Path: The path to the downloaded file or the existing file handle.

        Raises:
            Exception: If the file download fails or an unexpected error occurs.
        """
        try:
            if isinstance(self.file_handle, dxpy.DXFile):
                if self._destination is None:
                    file_path = download_dxfile_by_name(self.input_str)
                    return Path(file_path)
                elif self._destination.exists():
                    # if the destination already exists
                    file_path = self._destination
                    return Path(file_path)
                else:
                    # download file to destination
                    file_path = dxpy.download_dxfile(self.input_str, str(self._destination))
                    return Path(file_path)

            # Handle local path in the current directory
            path = Path(self.input_str)
            if path.exists() and path.is_file():
                return path.resolve()

            # Handle destination if it already exists
            if self._destination and self._destination.exists():
                return self._destination

            # Download using gsutil
            gsutil_cmd = f"gsutil cp {self.input_str} ."
            result = subprocess.run(gsutil_cmd, shell=True, check=True, text=True, capture_output=True)
            self._logger.info(f"Downloaded file using gsutil: {result.stdout.strip()}")
            return Path(self.input_str).resolve()

        except Exception as e:
            self._logger.error(f"Failed to handle file: {e}")
            raise

    def _decide_filetype(self) -> Optional[Union[dxpy.DXFile, Path]]:
        """
        Determine if the input is a DNA Nexus file or a local path.

        This method checks the type of the input string and determines whether it is a DNA Nexus file ID,
        a local file path, or an existing DXFile or Path object. It handles the following cases:
        - If the input is None or 'None', it returns None.
        - If the input is already a DXFile or Path object, it returns the input as is.
        - If the input is a local path, it checks if the path is absolute and exists.
        - If the input is a DNA Nexus file ID, it validates and returns the corresponding DXFile object.
        - If the input is a file name, it searches for the file on DNA Nexus and returns the corresponding DXFile object.

        :return: A DXFile or Path object if the input is valid, otherwise raises an appropriate exception.
        :raises ValueError: If the local path is not absolute.
        :raises dxpy.exceptions.DXError: If the DNA Nexus file ID is invalid.
        :raises FileNotFoundError: If the file is not found locally or on DNA Nexus.
        """
        if self.input_str is None or self.input_str == 'None':
            return None

        # Handle existing DXFile or Path objects
        if isinstance(self.input_str, (dxpy.DXFile, Path)):
            return self.input_str

        # Handle local path first
        path = Path(self.input_str)
        if not path.is_absolute():
            path = path.resolve()

        if path.exists():
            return path

        # Check if it's a DNA Nexus file ID
        if re.fullmatch(r'file-\w{24}', self.input_str):
            try:
                dx_file = dxpy.DXFile(dxid=self.input_str)
                dx_file.describe()
                return dx_file
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
            return dxpy.DXFile(dxid=found['id'], project=found['project'])
        except dxpy.exceptions.DXSearchError:
            raise FileNotFoundError(f"File not found locally or on DNA Nexus: {self.input_str}")
