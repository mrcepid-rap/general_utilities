import dxpy
import pandas as pd
from general_utilities.mrc_logger import MRCLogger

class InputParser:

    def __init__(self, input_str: str) -> None:

        self._logger = MRCLogger(__name__).get_logger()

    def _download(self):

    def decide_filetype(input_str: Optional[Union[str, Path, dxpy.DXFile]]) -> Optional[Union[dxpy.DXFile, Path]]:
        """
        Parses an input and returns the appropriate file type.

        - If input_str is None or "None", returns None.
        - If input_str is a dxpy.DXFile, returns it directly.
        - If input_str is a Path, processes it accordingly.
        - If input_str is a string matching a DNANexus file ID, returns a dxpy.DXFile.
        - Otherwise, treats input_str as an absolute file path:
             * First, attempts to locate the file on DNANexus.
             * If not found on DNANexus, checks if the file exists locally.
             * Returns a Path if it exists locally.

        :param input_str: The input representing a DNANexus file ID, a local file path, or already a dxpy.DXFile.
        :return: Either a dxpy.DXFile or a pathlib.Path.
        :raises FileNotFoundError: If the file is not found on DNANexus or locally.
        :raises ValueError: If a provided file path is not absolute.
        :raises TypeError: If a DNANexus file ID is invalid.
        """
        # Handle the case of a "None" input.
        if input_str is None or input_str == 'None':
            return None

        # If it's already a dxpy.DXFile or a Path, return it (or decide if you want to re-validate)
        if isinstance(input_str, dxpy.DXFile) or isinstance(input_str, Path):
            return input_str

        # Now, assume input_str is a string.
        # Case 1: DNANexus file ID.
        if re.fullmatch(r'file-\w{24}', input_str):
            try:
                dxfile = dxpy.DXFile(dxid=input_str)
                dxfile.describe()  # Validates that the file exists on DNANexus.
                return dxfile
            except (dxpy.exceptions.DXError, dxpy.exceptions.ResourceNotFound) as e:
                raise TypeError(f"Invalid DNANexus file ID: {input_str}") from e

        # Case 2: File path.
        file_handle = Path(input_str)
        if not file_handle.is_absolute():
            raise ValueError(f"Provided path '{input_str}' is not absolute. Please provide an absolute path.")

        try:
            # Attempt to locate the file on DNANexus.
            found_file = dxpy.find_one_data_object(
                classname='file',
                project=dxpy.PROJECT_CONTEXT_ID,
                name_mode='exact',
                name=file_handle.name,
                folder=str(file_handle.parent),
                zero_ok=False
            )
            return dxpy.DXFile(dxid=found_file['id'], project=found_file['project'])
        except dxpy.exceptions.DXSearchError:
            # If not found on DNANexus, check if the file exists locally.
            if file_handle.exists():
                logging.info(f"Local file '{input_str}' found.")
                return file_handle
            else:
                raise FileNotFoundError(
                    f"File '{input_str}' not found on DNANexus or locally."
                )
