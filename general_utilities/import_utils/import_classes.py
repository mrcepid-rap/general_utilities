from pathlib import Path
from typing import Union


class DXPath:
    """A simple helper class the stores both remote DX and local file Path objects

    This is technically a wrapper around Path that stores an additional Pathlike describing the location of the
    file on the DNANexus platform. .local and .remote access the local and remote Paths, respectively.

    :param remote_path: Path on the DNANexus filesystem
    :param local_path: Path on the local AWS instance, if not provided, will be placed in the root directory with the
        name given by :func:`remote.name`

    :ivar remote: Path on the DNANexus filesystem
    :ivar local: Path on the local AWS instance
    """

    def __init__(self, remote_path: Union[str, Path], local_path: Union[str, Path] = None):

        self.remote = Path(remote_path)
        if local_path is None:
            self.local = Path(f'./{self.remote.name}')
        else:
            self.local = Path(local_path)
