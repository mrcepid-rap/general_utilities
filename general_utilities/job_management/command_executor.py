import shlex
import subprocess
from pathlib import Path
from typing import Union, List

import dxpy

from general_utilities.mrc_logger import MRCLogger


class DockerMount:

    def __init__(self, local: Path, remote: Path):
        """An Object containing necessary information for creating a Docker mount point.

        Mount points are intended to be used as part of the -v flag within Docker. The only information supplied to
        the constructor is the local file path to the mounted directory, and the location within the Docker image.
        Calling the :func: get_docker_mount method will format these directories to be useable by Docker:

        <self.local>:<self.remote>

        :param local: A local file path to mount within a Docker image
        :param remote: The path within the Docker image
        """

        self.local = local
        self.remote = remote

    def get_docker_mount(self):
        """Utility method to get the mount in Docker -v format.

        :return: A str in the format `<self.local>:<self.remote>`
        """

        return f'{self.local.resolve()}:{self.remote}'


class CommandExecutor:
    """An object that contains the information to run system calls either with or without Docker

    For future developers: Why did Eugene create this seemingly stupid piece of code? Three reasons:

    1. The previous general_utilities.association_resources.run_cmd() method (that may or may not still exist) was
    inflexible with regard to running tests / prototyping OUTSIDE DNANexus.

    2. run_cmd() had to be parameterized during each use. That meant that one had to remember the exact Docker image to
    use at each invocation and the mount points within that image.

    3. Each module had to independently download a specified Docker image. While not a big deal, I wanted to modularise
    this process and make a call-out so code wasn't duplicated.

    :param docker_image: Docker image on some repository to run the command via. This image does not necessarily have
        to be on the image, but if in a non-public repository (e.g., AWS ECR) this will cause the command to fail.
    :param docker_mounts: Additional Docker mounts to attach to this process via the `-v` commandline argument to
        Docker. See the documentation for Docker for more information.
    :param aws_credentials: Path to AWS credentials to authenticate to AWS ECR for purposes of pulling a Docker image.
    """

    def __init__(self, docker_image: str = None, docker_mounts: List[DockerMount] = None,
                 aws_credentials: Path = None):

        self._logger = MRCLogger(__name__).get_logger()

        if aws_credentials:
            self._logger.info('Authenticating to AWS ECR')
            self._authenticate_aws_ecr(aws_credentials)

        self._docker_image = docker_image
        self._docker_configured = self._ingest_docker_file(docker_image)
        self._docker_prefix = self._construct_docker_prefix(docker_mounts)

    def _authenticate_aws_ecr(self, aws_credentials: Path) -> None:
        """Place files required for AWS-ECR authentication in the correct paths for Docker to find them.

        This handles the credentials provided by the record given in the 'assetDepends' portion of dxapp.json This
        asset MUST include a config.json and AWS formatted credentials file to function properly or this will throw an
        error!

        :param aws_credentials: Path to AWS credentials to authenticate to AWS ECR for purposes of pulling a Docker
            image.
        :return: None
        """

        # The config.json file is NOT provided by the user and is generated here.
        docker_config = Path('~/.docker/config.json')
        if not docker_config.expanduser().parent.exists():
            docker_config.expanduser().parent.mkdir()
        else:
            self._logger.warning('Docker config already exists. Overwriting!')
        with docker_config.expanduser().open('w') as config_writer:
            config_writer.write('{"credsStore": "ecr-login"}')

        # The credentials file is provided as part of DNANexus input. Here we need to move the file provided on the
        # command line (aws_credentials) to the correct PATH for Docker to find it.
        credentials_config = Path('~/.aws/credentials')
        if not credentials_config.expanduser().parent.exists():
            credentials_config.expanduser().parent.mkdir()
        aws_credentials.replace(credentials_config.expanduser())

    def _ingest_docker_file(self, docker_image: str) -> bool:
        """Download a Docker image (if requested) so that we can run tools not on the DNANexus platform.

        This method does a quick check using `docker image inspect <image>` to see if the image has already been
        downloaded to this machine to ensure that we don't waste time checking the remote repository. This is not
        particularly important, but does save a few seconds.

        :return: Boolean for if a docker image was provided to the constructor
        """

        if docker_image:

            cmd = f'docker image inspect {docker_image}'
            return_code = self.run_cmd(cmd, ignore_error=True)

            if return_code != 0:
                self._logger.info(f'Downloading Docker image {docker_image}')

                cmd = f'docker pull {docker_image}'
                self.run_cmd(cmd)

            return True

        else:
            self._logger.warning('No Docker image requested. Running via Docker is not available!')

            return False

    def _construct_docker_prefix(self, docker_mounts: List[DockerMount]) -> Union[str, None]:
        """Given a set of (possibly Null) docker mounts, construct the prefix for running Docker-based commands for
        this particular object.

        If a docker image was not provided at start-up, this method will return 'None' and all calls to Docker will
        raise an exception.

        Docker prefixes constructed by this method generally come with the following format:

        docker run -v /path/to/local_dir_1/:/path/to/mount_1/ -v /path/to/local_dir_2/:/path/to/mount_2/

        Note that the docker image itself is not added to this prefix to allow for additional mounts to be added later.

        :param docker_mounts: A List of DockerMount objects
        :return: A str representation of the docker prefix or None if no Docker image is provided.
        """

        if self._docker_configured:

            # -v here mounts a local directory on an instance (in this case the home dir) to a directory internal to the
            # Docker instance named /test/. This allows us to run commands on files stored on the AWS instance within
            # Docker. Multiple mounts can be added (via docker_mounts) to enable this code to find other specialised
            # files (e.g., some R scripts included in the associationtesting suite).
            if docker_mounts is None:
                docker_mount_string = ''
            else:
                docker_mount_string = ' '.join([f'-v {mount.get_docker_mount()}'
                                                for mount in docker_mounts])

            docker_prefix = f'docker run ' \
                            f'{docker_mount_string} '

            return docker_prefix

        else:

            return None

    def run_cmd_on_docker(self, cmd: str, stdout_file: Path = None, docker_mounts: List[DockerMount] = None,
                          print_cmd: bool = False, livestream_out: bool = False, dry_run: bool = False,
                          ignore_error: bool = False) -> int:
        """
        Run a command in the shell with Docker, automatically mounting parent directories of any absolute file paths
        found in the command string so files can be referenced by their full path inside Docker.

        We are now using absolute paths for all files we run on Docker. Filepaths are created using InputFileHandler.
        This means that we can automatically mount parent directories of absolute paths found in the command string.

        This method is a wrapper around the existing run_cmd method that automatically constructs the full Docker
        command to run the requested process within Docker.

        NOTE: if your command is failing, check whether or not you are supplying full paths as arguments. If you used
        InputFileHandler to create your input files, you should be fine. If you are creating files manually, you must
        ensure that you are using absolute paths with Path(some_file.txt).

        :param cmd: The command to be run.
        :param stdout_file: Capture stdout from the process into the given file
        :param docker_mounts: A List of additional docker mounts (as DockerMount objects) to add to this command.
        :param print_cmd: Print `cmd` but still run the command (as opposed to dry_run). For debug purposes only.
        :param livestream_out: Livestream the output from the requested process. For debug purposes only.
        :param dry_run: Print `cmd` and exit without running. For debug purposes only.
        :param ignore_error: Should failing subprocesses be ignored [False]? Setting to True allows the method to
            capture the returned error code and handle in a context dependent manner.
        :return: The exit code of the underlying process
        """

        if not self._docker_configured:
            raise dxpy.AppError('Requested to run via docker without configuring a Docker image!')

        try:
            # Safely split the command into respective parts (e.g. --input "some file.txt" -> ['--input', 'some file.txt'])
            command_arguments = shlex.split(cmd)
        except ValueError:
            # If shlex fails, fall back to a simple split (not sure if this will ever be needed)
            command_arguments = []

        # Extract parent directories of absolute paths to mount
        parent_dirs = {
            # Only consider absolute paths
            (Path(argument) if Path(argument).is_dir() else Path(argument).parent).resolve()
            if argument.startswith("/") else None
            for argument in command_arguments
        }
        # Remove None values (non-absolute paths)
        parent_dirs.discard(None)

        # Create DockerMounts for each parent directory of absolute file paths found in the command
        auto_mounts = [DockerMount(dir_path, dir_path) for dir_path in parent_dirs]
        all_mounts = (docker_mounts or []) + auto_mounts

        # Deduplicate mounts based on (local, remote) pairs
        deduped_mounts = list({(str(mount.local.resolve()), str(mount.remote)): mount for mount in all_mounts}.values())

        # Construct the full Docker command
        docker_mount_string = ' '.join(
            ['-v {}'.format(unique_mount.get_docker_mount()) for unique_mount in deduped_mounts])
        full_cmd = f'docker run {docker_mount_string} {self._docker_image} {cmd}'

        # Run the command using the existing run_cmd method
        return self.run_cmd(full_cmd, stdout_file, print_cmd, livestream_out, dry_run, ignore_error)

    def run_cmd(self, cmd: str, stdout_file: Path = None, print_cmd: bool = False,
                livestream_out: bool = False, dry_run: bool = False, ignore_error: bool = False) -> int:
        """Run a command in the shell.

        This method is the primary entrypoint to running system commands. By default, standard out is not saved,
        but can be modified with the 'stdout_file' parameter. print_cmd, livestream_out, and/or dry_run are for
        internal debugging purposes when testing new code. All options other than `cmd` are optional.

        By default, if a command fails, the VM will print the failing process STDOUT / STDERR to the logger and raise
        a RuntimeError; however, if ignore_error is set to 'True', this method will instead return the exit code for
        the underlying process to allow for custom error handling.

        :param cmd: The command to be run.
        :param stdout_file: Capture stdout from the process into the given file
        :param print_cmd: Print `cmd` but still run the command (as opposed to dry_run). For debug purposes only.
        :param livestream_out: Livestream the output from the requested process. For debug purposes only.
        :param dry_run: Print `cmd` and exit without running. For debug purposes only.
        :param ignore_error: Should failing subprocesses be ignored [False]? Setting to True allows the method to
            capture the returned error code and handle in a context dependent manner.
        :return: The exit code of the underlying process
        """

        return self._execute_cmd(cmd, stdout_file, print_cmd, livestream_out, dry_run, ignore_error)

    def _execute_cmd(self, cmd: str, stdout_file: Path, print_cmd: bool, livestream_out: bool,
                     dry_run: bool, ignore_error: bool) -> int:
        """A private method for executing commands via the shell. See 'run_cmd' for more information on providing
        inputs to this command.

        :param cmd: The command to be run.
        :param stdout_file: Capture stdout from the process into the given file
        :param print_cmd: Print `cmd` but still run the command (as opposed to dry_run). For debug purposes only.
        :param livestream_out: Livestream the output from the requested process. For debug purposes only.
        :param dry_run: Print `cmd` and exit without running. For debug purposes only.
        :param ignore_error: Should failing subprocesses be ignored [False]? Setting to True allows the method to
            capture the returned error code and handle in a context dependent manner.
        :return: The exit code of the underlying process
        """

        if dry_run:
            self._logger.info(cmd)
            return 0
        else:
            if print_cmd:
                self._logger.info(cmd)

            # Standard python calling external commands protocol
            proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            # Trying to do both simultaneously to make code more succinct.
            if livestream_out or stdout_file is not None:

                # If stdout is not provided convert to /dev/null, so we can do livestreaming and writing to stdout at
                # the same time
                stdout_file = stdout_file if stdout_file else Path('/dev/null')

                stdout_writer = stdout_file.open('w')
                for line in iter(proc.stdout.readline, b""):
                    decoded_bytes = line.decode('utf-8')
                    if livestream_out:
                        self._logger.info(f'SUBPROCESS STDOUT: {decoded_bytes.rstrip()}')
                    stdout_writer.write(decoded_bytes)

            # Wait for the process to finish
            proc_exit_code = proc.wait()

            # If the process has a non-zero exit code, dump information about the job. Depending on ignore_error, can
            # either raise a RuntimeError (False) or return the exit code for another process to handle (True)
            if proc_exit_code != 0 and ignore_error is False:
                self._logger.error("The following cmd failed:")
                self._logger.error(cmd)
                self._logger.error("STDOUT follows")
                for line in iter(proc.stdout.readline, b""):
                    self._logger.error(line.decode('utf-8'))
                self._logger.error("STDERR follows\n")
                for line in iter(proc.stderr.readline, b""):
                    self._logger.error(line.decode('utf-8'))
                raise RuntimeError(f'run_cmd() failed to run requested job properly')

            return proc_exit_code


def build_default_command_executor() -> CommandExecutor:
    """
    Returns a `CommandExecutor` instance.
    The default implementation does not auto-mount parent directories of absolute file paths.
    """
    # no default mount
    cmd_executor = CommandExecutor(
        docker_image='egardner413/mrcepid-burdentesting:latest',
        docker_mounts=[]
    )
    return cmd_executor
