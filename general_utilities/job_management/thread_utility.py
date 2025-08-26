import math
import os
from concurrent import futures
from concurrent.futures import ThreadPoolExecutor, Future
from typing import Any, Iterator, Callable, List, Dict, Optional

import dxpy

from general_utilities.job_management.joblauncher_interface import JobLauncherInterface


class ThreadUtility(JobLauncherInterface):

    def __init__(self,
                 incrementor: int = 500,
                 threads: int = None,
                 thread_factor: int = 1):

        super().__init__(incrementor=incrementor,
                         concurrent_job_limit=self._decide_concurrent_job_limit(threads, thread_factor))

        self._executor = ThreadPoolExecutor(max_workers=self._concurrent_job_limit)

    def __iter__(self) -> Iterator:

        if len(self._output_array) == 0:
            raise dxpy.AppError('No jobs submitted to future pool!')

        self._logger.info("{0:65}: {val}".format("Total number of threads to iterate through", val=self._total_jobs))

        self._output_array = futures.as_completed(self._job_queue)
        return self

    def launch_job(self, function: Callable, inputs: Optional[Dict[str, Any]] = None,
                   outputs=None, name=None, instance_type=None, **kwargs) -> None:
        """
        Launch a job by submitting it to the thread executor.
        """
        if self._queue_closed:
            raise dxpy.AppError("Thread executor has already been collected from!")

        # Track job count for status reporting
        self._total_jobs += 1

        params: Dict[str, Any] = inputs if inputs is not None else dict(kwargs)
        self._output_array.append((function, params))

    def submit_and_monitor(self) -> List[Future]:
        """
        Submit the queued jobs and return an iterator over the futures.
        """

        if len(self._output_array) == 0:
            raise dxpy.AppError('No jobs submitted to future pool!')

        self._logger.info("{0:65}: {val}".format("Total number of threads to iterate through", val=self._total_jobs))

        # Submit the collected jobs to the executor
        self._job_queue = [
            self._executor.submit(function, **inputs) for function, inputs in self._output_array
        ]

        # Mark the queue as closed after successful submission
        self._queue_closed = True

        return self._job_queue

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
