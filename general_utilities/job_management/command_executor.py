import subprocess
import sys

import dxpy

from typing import Union, List
from pathlib import Path

from general_utilities.mrc_logger import MRCLogger


class DockerMount:

    def __init__(self, local: Path, remote: Path):

        self.local = local
        self.remote = remote

    def get_docker_mount(self):

        return f'{self.local}:{self.remote}'


class CommandExecutor:
    """An object that contains the information to run system calls either with or without docker

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
        """Download the default Docker image so that we can run tools not on the DNANexus platform.

        :return: None
        """

        if docker_image:
            self._logger.info(f'Downloading Docker image {docker_image}')

            cmd = f'docker pull {docker_image}'
            self.run_cmd_on_local(cmd)

            return True

        else:
            self._logger.warning('No Docker image requested. Running via Docker is not available!')

            return False

    def _construct_docker_prefix(self, docker_mounts: List[DockerMount]) -> Union[str, None]:

        if self._docker_configured:

            # -v here mounts a local directory on an instance (in this case the home dir) to a directory internal to the
            # Docker instance named /test/. This allows us to run commands on files stored on the AWS instance within
            # Docker. Multiple mounts can be added (via docker_mounts) to enable this code to find other specialised
            # files (e.g., some R scripts included in the associationtesting suite).
            if docker_mounts is None:
                docker_mount_string = ' '
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
                          print_cmd: bool = False, livestream_out: bool = False, dry_run: bool = False) -> int:
    
        """Run a command in the shell with Docker
    
        This function runs a command on an instance via the subprocess module with a Docker instance we downloaded;
        This command will return an error if a valid Docker image was not provided. Docker images are run in headless
        mode, which cannot be modified. Additional mount points inside the VM can be provided at runtime via the
        docker_mounts option. Also, by default, standard out is not saved, but can be modified with the 'stdout_file'
        parameter. print_cmd, livestream_out, and/or dry_run are for internal debugging purposes when testing new
        code. All options other than `cmd` are optional.
    
        :param cmd: The command to be run.
        :param stdout_file: Capture stdout from the process into the given file
        :param docker_mounts: A List of additional docker mounts (as DockerMount objects) to add to this command.
        :param print_cmd: Print `cmd` but still run the command (as opposed to dry_run). For debug purposes only.
        :param livestream_out: Livestream the output from the requested process. For debug purposes only.
        :param dry_run: Print `cmd` and exit without running. For debug purposes only.
        :returns: The exit code of the underlying process
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

        return self._execute_cmd(cmd, stdout_file, print_cmd, livestream_out, dry_run)

    def run_cmd_on_local(self, cmd: str, stdout_file: Path = None,
                         print_cmd: bool = False, livestream_out: bool = False, dry_run: bool = False) -> int:

        return self._execute_cmd(cmd, stdout_file, print_cmd, livestream_out, dry_run)

    def _execute_cmd(self, cmd: str, stdout_file: Path, print_cmd: bool, livestream_out: bool,
                     dry_run: bool):

        if dry_run:
            self._logger.info(cmd)
            return 0
        else:
            if print_cmd:
                self._logger.info(cmd)
    
            # Standard python calling external commands protocol
            proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if livestream_out:
    
                for line in iter(proc.stdout.readline, b""):
                    self._logger.info(f'SUBPROCESS STDOUT: {bytes.decode(line).rstrip()}')
    
                proc.wait()  # Make sure the process has actually finished...
                if proc.returncode != 0:
                    self._logger.error("The following cmd failed:")
                    self._logger.error(cmd)
                    self._logger.error("STDERR follows\n")
                    for line in iter(proc.stderr.readline, b""):
                        sys.stdout.buffer.write(line)
                    raise dxpy.AppError("Failed to run properly...")
    
            else:
                stdout, stderr = proc.communicate()
                if stdout_file is not None:
                    with Path(stdout_file).open('w') as stdout_writer:
                        stdout_writer.write(stdout.decode('utf-8'))
                    stdout_writer.close()
    
                # If the command doesn't work, print the error stream and close the AWS instance out with 'dxpy.AppError'
                if proc.returncode != 0:
                    self._logger.error("The following cmd failed:")
                    self._logger.error(cmd)
                    self._logger.error("STDOUT follows")
                    self._logger.error(stdout.decode('utf-8'))
                    self._logger.error("STDERR follows")
                    self._logger.error(stderr.decode('utf-8'))
                    raise RuntimeError(f'run_cmd() failed to run requested job properly')
    
            return proc.returncode
