import dxpy

from urllib import request
from pathlib import Path

from general_utilities.job_management.command_executor import CommandExecutor
from general_utilities.mrc_logger import MRCLogger

LOGGER = MRCLogger().get_logger()


def mount_dx_project(data_project: str = None) -> Path:
    """Mounts a DNAnexus project to the local filesystem using dxfuse.

    For more information on dxfuse, see the dxfuse documentation at:

    https://github.com/dnanexus/dxfuse

    The above documentation is specific to a beta version of dxfuse (~v0.21.0) and has not been updated to reflect
    the hard-coded version used here (v1.2.0).

    Note that DNANexus prolifically uses file paths with spaces. When using file paths mounted using this method, be
    sure to use the appropriate escape characters and/or quotes when passing file paths to external system calls.

    :param data_project: The DNAnexus project to mount. If not provided, the current project will be used.
    :return: The path to the project-specific directory mounted using dxfuse.
    """

    LOGGER.info('Mounting DNANexus project data...')

    # Download DXFuse so we can mount the filesystem locally
    dxfuse_url = 'https://github.com/dnanexus/dxfuse/releases/download/v1.2.0/dxfuse-linux'
    dxfuse_path = Path('./dxfuse-linux')
    request.urlretrieve(dxfuse_url, dxfuse_path)
    dxfuse_path.chmod(0o557)

    # And then make the project directory
    mount_dir = Path('mount/')
    if not mount_dir.exists():
        mount_dir.mkdir()

    # And mount it...
    if data_project:
        current_project = data_project
    else:
        current_project = dxpy.PROJECT_CONTEXT_ID

    current_project_name = dxpy.describe(current_project)['name']
    CommandExecutor().run_cmd(f'{dxfuse_path.resolve()} {mount_dir.resolve()} {current_project}')

    mount_path = Path(f'mount/Bulk/{current_project_name}')

    if mount_path.exists() and mount_path.is_dir():
        LOGGER.info(f'dxfuse mounted successfully at {mount_path}...')
    else:
        raise FileNotFoundError('Mount point does not exist or is not a directory...')

    return mount_path
