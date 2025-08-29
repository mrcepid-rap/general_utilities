import math
import os
from concurrent import futures
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Dict, Optional

import dxpy

from general_utilities.job_management.joblauncher_interface import JobLauncherInterface, JobInfo


class ThreadUtility(JobLauncherInterface):

    def __init__(self,
                 incrementor: int = 500,
                 threads: int = None,
                 thread_factor: int = 1):

        super().__init__(incrementor=incrementor,
                         concurrent_job_limit=self._decide_concurrent_job_limit(threads, thread_factor))

        self._executor = ThreadPoolExecutor(max_workers=self._concurrent_job_limit)

    def launch_job(self,
                   function: Callable,
                   inputs: Optional[Dict[str, Any]] = None,
                   outputs=None,
                   **kwargs) -> None:
        """
        Queue a job for later submission, harmonized with SubjobUtility.

        :param function: The function to be executed in the thread.
        :param inputs: A dictionary of input parameters to be passed to the function.
        :param outputs: A list of expected output names (not used in this implementation).
        :param kwargs: Additional keyword arguments (not used in this implementation).
        """
        if self._queue_closed:
            raise dxpy.AppError("Thread executor has already been collected from!")

        self._total_jobs += 1

        if outputs is None:
            outputs = []

        # Create job info dictionary
        input_parameters: JobInfo = {
            'function': function,
            'properties': {},
            'input': inputs,
            'outputs': outputs,
            'job_type': None,
            'destination': None,
            'name': None,
            'instance_type': None
        }

        # Append job info to the job queue
        self._job_queue.append(input_parameters)

    def submit_and_monitor(self) -> None:
        """
        Submit the queued jobs and monitor their execution.
        """
        if not self._job_queue:
            raise dxpy.AppError('No jobs submitted to future pool!')

        self._queue_closed = True

        self._logger.info("{0:65}: {val}".format(
            "Total number of threads to iterate through", val=self._total_jobs
        ))

        futures_list = []
        # Submit each job in the queue to the executor
        for job in self._job_queue:
            inputs = job['input']
            function = job['function']
            # Submit the job to the executor
            submission = self._executor.submit(function, **inputs)
            # Append the future object to the futures list
            futures_list.append(submission)

        # Monitor the completion of the submitted jobs
        for output in futures.as_completed(futures_list):
            # Append the result of each completed job to the output array
            self._output_array.append(output.result())
            self._num_completed_jobs += 1
            self._print_status()

    def _decide_concurrent_job_limit(self, requested_threads: int, thread_factor: int) -> int:
        """
        Get the number of concurrent jobs that can be run at one time using either the provided number of threads or the
        number of threads available on the machine.

        :param requested_threads: The number of threads requested by the user.
        :param thread_factor: The number of threads required per job in this thread pool.
        :return: The number of concurrent jobs that can be used on this machine
        """

        threads = requested_threads if requested_threads else os.cpu_count()
        if threads is None or threads < 1:
            self._logger.error('Not enough threads on machine to complete task. Number of threads on this machine '
                               f'({threads}) is less than 1.')
            raise ValueError('Not enough threads on machine to complete task.')

        available_workers = math.floor(threads / thread_factor)
        if available_workers < 1:
            self._logger.error('Not enough threads on machine to complete task. Number of threads on this machine '
                               f'({threads}) is less than {thread_factor}.')
            raise ValueError('Not enough threads on machine to complete task.')

        return available_workers
