from general_utilities.job_management.joblauncher_interface import JobLauncherInterface
from general_utilities.job_management.subjob_utility import SubjobUtility
from general_utilities.job_management.thread_utility import ThreadUtility
from general_utilities.platform_utils.platform_factory import PlatformFactory, Platform


def joblauncher_factory(incrementor: int = 500, concurrent_job_limit: int = 100,
                        download_on_complete: bool = False, threads: int = None,
                        thread_factor: int = 1) -> JobLauncherInterface:
    """
    Job launcher that uses either SubjobUtility for DX or ThreadUtility for local execution.
    GCP is not supported in this implementation.

    This class detects the platform and initializes the appropriate backend utility for job management.
    It provides methods to launch jobs, submit and monitor them, and retrieve outputs.

    :param concurrent_job_limit: The maximum number of concurrent jobs to run, default is 100.
    :param retries: The number of retries for job submission, default is 1.
    :param incrementor: The incrementor for job submission, default is 500.
    :param download_on_complete: Whether to download outputs on job completion, default is False.
    :param thread_factor: The factor to multiply the number of threads by for local execution, default is 1.
    :param threads: The number of threads to use for local execution, default is None.
    :raises RuntimeError: If an unsupported platform is detected (GCP not currently implemented).
    """

    platform = PlatformFactory().get_platform()

    if platform == Platform.DX:
        backend = SubjobUtility(incrementor=incrementor,
                                concurrent_job_limit=concurrent_job_limit,
                                download_on_complete=download_on_complete)
    elif platform == Platform.LOCAL:
        backend = ThreadUtility(incrementor=incrementor,
                                threads=threads,
                                thread_factor=thread_factor)
    else:
        raise RuntimeError("Unsupported platform")
    return backend
