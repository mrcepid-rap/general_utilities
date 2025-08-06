import math
from asyncio import Future
from concurrent import futures
from concurrent.futures import ThreadPoolExecutor
from time import sleep
from typing import Any, Iterator, Callable

import dxpy

from general_utilities.job_management.job_launcher_interface import JobLauncherInterface
from general_utilities.mrc_logger import MRCLogger


class ThreadUtility(JobLauncherInterface):

    def __init__(self, threads: int = None, thread_factor: int = 1, **kwargs):

        super().__init__(threads=threads, **kwargs)

        self._logger = MRCLogger(__name__).get_logger()

        self._already_collected = False  # A flag to make sure we don't submit jobs to a closed executor
        self._num_jobs = 0
        self._total_finished_models = 0

        available_workers = math.floor(self._threads / thread_factor)
        self._executor = ThreadPoolExecutor(max_workers=available_workers)
        self._future_pool = []

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

        self._already_collected = True

        if len(self._future_pool) == 0:
            raise dxpy.AppError('No jobs submitted to future pool!')

        self._logger.info("{0:65}: {val}".format("Total number of threads to iterate through", val=self._num_jobs))

        self._future_iterator = futures.as_completed(self._future_pool)
        return self

    def submit_and_monitor(self) -> Iterator[Future]:
        """
        Submit the queued jobs and return an iterator over the futures.
        """

        self._already_collected = True

        if len(self._future_pool) == 0:
            raise dxpy.AppError('No jobs submitted to future pool!')

        self._logger.info("{0:65}: {val}".format("Total number of threads to iterate through", val=self._num_jobs))

        return futures.as_completed(self._future_pool)

    def launch_job(self, function: Callable, **kwargs) -> None:
        """
        Launch a job by submitting it to the thread executor.
        """
        if self._already_collected:
            raise dxpy.AppError("Thread executor has already been collected from!")
        else:
            self._num_jobs += 1
            self._future_pool.append(self._executor.submit(function,
                                                           **kwargs))

    # This is a utility method that will essentially 'hold' until all threads added to this class are completed.
    # It just makes it so if one does not need to access the futures, there is no need to implement an empty for loop
    # in your code. Since 'self' represents this class, and this class implements __iter__, it will run the code in
    # the __iter__ class, which will hold until all jobs are completed.
    def collect_futures(self) -> None:
        for _ in self:
            pass
