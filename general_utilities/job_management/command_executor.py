import dxpy
import subprocess

from pathlib import Path
from typing import Union, List

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

        return f'{self.local}:{self.remote}'


class CommandExecutor:
    """An object that contains the information to run system calls either with or without Docker

    :param docker_image: Docker image on some repository to run the command via. This image does not necessarily have
        to be on the image, but if in a non-public repository (e.g., AWS ECR) this will cause the command to fail.
    :param docker_mounts: Additional Docker mounts to attach to this process via the `-v` commandline argument to
        Docker. See the documentation for Docker for more information.
    """
    
    def __init__(self, docker_image: str = None, docker_mounts: List[DockerMount] = None):

        self._logger = MRCLogger(__name__).get_logger()

        self._docker_image = docker_image
        self._docker_configured = self._ingest_docker_file(docker_image)
        self._docker_prefix = self._construct_docker_prefix(docker_mounts)

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
            print(f'Code: {return_code}')
            if return_code != 0:

                self._logger.info(f'Downloading Docker image {docker_image}')

                cmd = f'docker pull {docker_image}'
                self.run_cmd(cmd)

            else:

                self._logger.info(f'Docker image {docker_image} already downloaded...')

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

            self._logger.info(f'Docker will be run with prefix \'{docker_prefix}\'')

            return docker_prefix

        else:

            return None

    def run_cmd_on_docker(self, cmd: str, stdout_file: Path = None, docker_mounts: List[DockerMount] = None,
                          print_cmd: bool = False, livestream_out: bool = False, dry_run: bool = False,
                          ignore_error: bool = False) -> int:
    
        """Run a command in the shell with Docker
    
        This function runs a command on an instance via the subprocess module with a Docker instance we downloaded;
        This command will return an error if a valid Docker image was not provided. Docker images are run in headless
        mode, which cannot be modified. Additional mount points inside the VM can be provided at runtime via the
        docker_mounts option. Also, by default, standard out is not saved, but can be modified with the 'stdout_file'
        parameter. print_cmd, livestream_out, and/or dry_run are for internal debugging purposes when testing new
        code. All options other than `cmd` are optional.

        This method is a wrapper around CommandExecutor.run_cmd() and simply adds self._docker_prefix and
        self._docker_image to the beginning of any provided command.

        By default, if a command fails, the VM will print the failing process STDOUT / STDERR to the logger and raise
        a RuntimeError; however, if ignore_error is set to 'True', this method will instead return the exit code for
        the underlying process to allow for custom error handling.
    
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

        if self._docker_configured is False:
            raise dxpy.AppError('Requested to run via docker without configuring a Docker image!')

        # -v here mounts a local directory on an instance (in this case the home dir) to a directory internal to the
        # Docker instance named /test/. This allows us to run commands on files stored on the AWS instance within
        # Docker. Multiple mounts can be added (via docker_mounts) to enable this code to find other specialised
        # files (e.g., some R scripts included in the associationtesting suite).
        docker_mount_string = ' '.join([f'-v {mount.get_docker_mount()}'
                                        for mount in docker_mounts])

        # Use the original docker prefix created as part of the constructor with any additional mounts provided to
        # this method
        cmd = f'{self._docker_prefix} {docker_mount_string} {self._docker_image} {cmd}'
        return self.run_cmd(cmd, stdout_file, print_cmd, livestream_out, dry_run, ignore_error)

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

                # If stdout is not provided convert to /dev/null
                stdout_file = stdout_file if stdout_file else Path('/dev/null')

                stdout_writer = stdout_file.open('w')
                for line in iter(proc.stdout.readline, b""):
                    decoded_bytes = line.decode('utf-8')
                    if livestream_out:
                        self._logger.info(f'SUBPROCESS STDOUT: {decoded_bytes.rstrip()}')
                    stdout_writer.write(decoded_bytes)

            # Wait for the process to finish
            proc_exit_code = proc.wait()
            print(f'Internal: {proc_exit_code}')
            # If the process has a non-zero exit code, dump information about the job. Depending on ignore_error, can
            # either raise a RuntimeError (False) or return the exit code for another process to handle (True)
            if proc_exit_code != 0:
                self._logger.error("The following cmd failed:")
                self._logger.error(cmd)
                self._logger.error("STDOUT follows")
                for line in iter(proc.stdout.readline, b""):
                    self._logger.error(line)
                self._logger.error("STDERR follows\n")
                for line in iter(proc.stderr.readline, b""):
                    self._logger.error(line)

                if not ignore_error:
                    raise RuntimeError(f'run_cmd() failed to run requested job properly')

            return proc_exit_code
