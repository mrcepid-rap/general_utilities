import math
from concurrent import futures
from concurrent.futures import ThreadPoolExecutor, Future
from time import sleep
from typing import Any, Iterator, Callable

import dxpy

from general_utilities.job_management.job_launcher_interface import JobLauncherInterface


class ThreadUtility(JobLauncherInterface):

    def __init__(self, incrementor: int = 500,
                 threads: int = None,
                 thread_factor: int = 1):

        super().__init__(incrementor=incrementor,
                         threads=threads)

        available_workers = math.floor(self._threads / thread_factor)
        self._executor = ThreadPoolExecutor(max_workers=available_workers)

    # I have a feeling the sleep() part is VERY inefficient but I am unsure of how to fix at the moment...
    # Due to how futures work, most of the time a next() call will receive a 'None' return. We need to create a
    # waiter that will hold until we get something OTHER than None, so we can return it to the main class.
    def __next__(self) -> Any:

        curr_future = next(self._future_iterator)
        while curr_future is None:
            curr_future = next(self._future_iterator)
            sleep(0.1)

        result = curr_future.result()
        return result

    def __iter__(self) -> Iterator:

        self._queue_closed = True

        if len(self._output_array) == 0:
            raise dxpy.AppError('No jobs submitted to future pool!')

        self._logger.info("{0:65}: {val}".format("Total number of threads to iterate through", val=self._total_jobs))

        self._future_iterator = futures.as_completed(self._output_array)
        return self

    def __len__(self) -> int:
        """
        Get the number of jobs in the queue.
        :return: The total number of jobs in the queue.
        """
        return len(self._output_array)

    def launch_job(self, function: Callable, inputs: dict, outputs=None, name=None, instance_type=None,
                   **kwargs) -> None:
        """
        Launch a job by submitting it to the thread executor.
        """
        if self._queue_closed:
            raise dxpy.AppError("Thread executor has already been collected from!")

        # Track job count for status reporting
        self._total_jobs += 1

        # Collect the job details in a queue
        self._output_array.append((function, inputs))

    def submit_and_monitor(self) -> list[Future]:
        """
        Submit the queued jobs and return an iterator over the futures.
        """

        if len(self._output_array) == 0:
            raise dxpy.AppError('No jobs submitted to future pool!')

        self._logger.info("{0:65}: {val}".format("Total number of threads to iterate through", val=self._total_jobs))

        self._print_status()

        # Submit the collected jobs to the executor
        submitted_futures = [
            self._executor.submit(function, **inputs) for function, inputs in self._output_array
        ]

        # Mark the queue as closed after successful submission
        self._queue_closed = True

        return submitted_futures
