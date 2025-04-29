from pathlib import Path
from typing import Union

import dxpy
from dxpy import DXSearchError

from general_utilities.mrc_logger import MRCLogger

LOGGER = MRCLogger(__name__).get_logger()


def find_dxlink(name: str, folder: str, project=None) -> dict:
    """This method is a simple wrapper for dxpy.find_one_data_object() for ease of repetitive use

    :param name: EXACT name of the file to be searched for (without path information)
    :param folder: EXACT name of the folder where this should be found
    :param project: The project ID to search in. If None, will use the current project context
    :return: A dxpy.dxlink() representation of the file
    """

    if project is None:
        project = dxpy.PROJECT_CONTEXT_ID

    try:
        dxlink = dxpy.dxlink(dxpy.find_one_data_object(name=name,
                                                       classname='file',
                                                       folder=folder,
                                                       project=project,
                                                       name_mode='exact',
                                                       zero_ok=False))
    except DXSearchError:
        raise FileNotFoundError(f'File – {folder}/{name} – not found during imputation data search!')

    return dxlink


def download_dxfile_by_name(file: Union[dict, str, dxpy.DXFile], project_id: str = None,
                            print_status: bool = False) -> Path:
    """Download a dxfile and downloads to the file 'name' as given by dxfile.describe()

    This method can take either:

    1. A DNANexus link (i.e., in the style provided to :func:`main` at startup)

    2. A dict from a 'find_objects()' call (has keys of 'id' and 'project')

    3. A string representation of a DNANexus file (e.g., file-12345...)

    4. A DNANexus file object from dxpy.DXFile

    And will then download this file to the local environment using the remote name of the file.

    :param file: A DNANexus link / file-ID string, or dxpy.DXFile object to download
    :param project_id: Optional project ID of the file to be downloaded. Only required if accessing bulk data or
        downloading a file from another project.
    :param print_status: Should this method print a message indicating that the provided file is being downloaded?
    :return: A Path pointing to the file on the local filesystem
    """
    if type(file) == dict:
        if 'id' in file:
            file = dxpy.DXFile(dxid=file['id'], project=file['project'])
        else:
            file = dxpy.DXFile(file)
    elif type(file) == str:
        file = dxpy.DXFile(file)

    curr_filename = file.describe()['name']

    if print_status:
        LOGGER.info(f'Downloading file {curr_filename} ({file.get_id()})')
    dxpy.download_dxfile(file.get_id(), curr_filename, project=project_id)

    return Path(curr_filename)


def generate_linked_dx_file(file: Union[str, Path], delete_on_upload: bool = True) -> dxpy.DXFile:
    """A helper function to upload a local file to the DNANexus platform and then remove it from the instance.

     A simple wrapper around :func:`dxpy.upload_local_file()` with additional functionality to remove the file from
     the local instance storage system.

     This will generate a dict with the format::

        {'$dnanexus_link': 'file-1234567890ABCDEFGabcdefg'}

    With default input this method also deletes the uploaded file on upload. This functionality can be changed by
    setting :param delete_on_upload: to False.

    :param file: Either a str or Path representation of the file to upload.
    :param delete_on_upload: Delete this file on upload? [True]
    :return: A :func:`dxpy.DXFile` instance of the remote file.
    """

    if type(file) == str:
        linked_file = dxpy.upload_local_file(filename=file)
        if delete_on_upload:
            Path(file).unlink()
    else:
        linked_file = dxpy.upload_local_file(file=file.open('rb'))
        if delete_on_upload:
            file.unlink()
    return linked_file
