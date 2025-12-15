import os
from pathlib import Path
from typing import List, Union

import dxpy
from dxpy import DXFile
from google.cloud import storage

from general_utilities.import_utils.file_handlers.dnanexus_utilities import generate_linked_dx_file
from general_utilities.mrc_logger import MRCLogger
from general_utilities.platform_utils.platform_factory import PlatformFactory, Platform


class ExportFileHandler:
    """
    This class is designed to recognize the platform on which it is running (Local, DNANexus, or GCP) and then
    upload files accordingly.

    For DNA Nexus, it converts the input files to DX links.
    For GCP, it manually uploads files to the location specified by the GCP_OUTPUT_URL env var and returns the gs:// path.
    For Local, it simply returns the local path.
    """

    def __init__(self, delete_on_upload: bool = True):

        self._logger = MRCLogger(__name__).get_logger()

        self._platform = PlatformFactory().get_platform()
        self._gcp_check_result = None
        self._delete_on_upload = delete_on_upload

    def _convert_file_to_dxlink(self, file: Union[str, Path, DXFile, dict]) -> dict:
        """
        Converts a file input to a DX link.
        """
        if isinstance(file, dxpy.DXFile):
            converted_file = dxpy.dxlink(file)
        elif isinstance(file, dict) and dxpy.is_dxlink(file):
            converted_file = file
        else:
            converted_file = dxpy.dxlink(
                generate_linked_dx_file(file, delete_on_upload=self._delete_on_upload)
            )
        return converted_file

    def _upload_gcp_file(self, file_path: Union[str, Path]) -> str:
        """
        Manually uploads a file to GCS and returns the gs:// path.
        Requires the 'GCP_OUTPUT_URL' environment variable to be set (e.g. gs://my-bucket/my-output-folder).
        """
        file_path = Path(file_path)
        output_url = os.environ.get('GCP_OUTPUT_URL')

        if not output_url:
            raise ValueError("GCP_OUTPUT_URL environment variable is missing. Cannot upload file.")

        if not output_url.startswith("gs://"):
            raise ValueError(f"GCP_OUTPUT_URL must start with 'gs://'. Found: {output_url}")

        # Parse bucket and prefix from the URL
        # gs://bucket_name/folder/subfolder -> parts: ['', '', 'bucket_name', 'folder', 'subfolder']
        parts = output_url.split('/')
        bucket_name = parts[2]
        # Join the rest to get the folder prefix. Filter empty strings to avoid double slashes.
        blob_prefix = "/".join([p for p in parts[3:] if p])

        # Define the full blob name (prefix + filename)
        destination_blob_name = f"{blob_prefix}/{file_path.name}" if blob_prefix else file_path.name

        self._logger.info(f"Uploading {file_path.name} to gs://{bucket_name}/{destination_blob_name}...")

        # Initialize client (uses dsub default credentials automatically)
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        blob.upload_from_filename(str(file_path))

        full_gs_path = f"gs://{bucket_name}/{destination_blob_name}"
        self._logger.info(f"Upload successful: {full_gs_path}")

        return full_gs_path

    def export_files(self, files_input: Union[str, Path, List[Union[str, Path]]]) -> Union[
        Union[str, Path], List[Union[str, Path]], List[dict], dict]:
        """
        Export files according to platform.
        """
        result = []

        if self._platform == Platform.LOCAL:
            self._logger.info("Local platform detected: returning raw file paths.")
            if isinstance(files_input, list):
                result = [Path(f) for f in files_input]
            else:
                result = Path(files_input)

        elif self._platform == Platform.GCP:
            self._logger.info("GCP platform detected: Uploading to GCS...")
            if isinstance(files_input, list):
                result = [self._upload_gcp_file(f) for f in files_input]
            else:
                result = self._upload_gcp_file(files_input)

        elif self._platform == Platform.DX:
            if isinstance(files_input, list):
                result = [self._convert_file_to_dxlink(f) for f in files_input]
            else:
                result = self._convert_file_to_dxlink(files_input)

        else:
            raise RuntimeError(f"Unsupported input type for export_files: {type(files_input)}")

        return result