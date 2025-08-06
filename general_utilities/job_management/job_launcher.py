from general_utilities.job_management.job_launcher_interface import JobLauncherInterface
from general_utilities.job_management.subjob_utility import SubjobUtility
from general_utilities.job_management.thread_utility import ThreadUtility
from general_utilities.mrc_logger import MRCLogger
from general_utilities.platform_utils.platform_factory import Platform, PlatformFactory


class JobLauncher(JobLauncherInterface):
    """
    Job launcher that uses either SubjobUtility for DX or ThreadUtility for local execution.
    GCP is not supported in this implementation.
    """

    def __init__(self, **kwargs):
        self._logger = MRCLogger(__name__).get_logger()

        self._platform = PlatformFactory().get_platform()
        self._logger.info(f"Detected platform: {self._platform.value}")

        super().__init__(incrementor=kwargs.get('incrementor', 500),
                         threads=kwargs.get('threads'))

        if self._platform == Platform.DX:
            self._backend = SubjobUtility(**kwargs)
        elif self._platform == Platform.LOCAL:
            self._backend = ThreadUtility(**kwargs)
        elif self._platform == Platform.GCP:
            raise NotImplementedError("GCP platform is not supported in this implementation.")
        else:
            raise RuntimeError("Unsupported platform, please seek help")

    def launch_job(self, function, inputs, outputs=None, name=None, instance_type=None):
        if self._platform == Platform.DX:
            self._backend.launch_job(function=function, inputs=inputs, outputs=outputs,
                                     name=name, instance_type=instance_type)
        elif self._platform == Platform.LOCAL:
            self._backend.launch_job(function=function, **inputs)

    def submit_queue(self):
        self._backend.submit_queue()

    def __iter__(self):
        return iter(self._backend)

    @property
    def platform(self):
        return self._platform
